"""Configuration de la couche Silver.

Tout ce qui dépend de l'environnement (chemins, catalogue, clé) passe par des
variables d'environnement, injectées par Airflow. Tout ce qui dépend du format
des sources (noms de colonnes, séparateurs, encodage, format des dates) est
regroupé dans SOURCES.

Les correspondances ci-dessous sont VÉRIFIÉES sur les données réelles du projet
(dossier DATA_2 : dump PostgreSQL DATA2023, CSV "Etablissement de SANTE",
Hospitalisations.csv, fichiers e-Satis HAS, deces.csv). Chaque champ accepte une
liste de candidats : la comparaison ignore la casse, les accents, les espaces et
les "_". Le premier candidat trouvé gagne (l'ordre compte, voir e-Satis).
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # Racine de la couche Bronze (déposée par NiFi)
    bronze_root: str = os.getenv("BRONZE_ROOT", "s3a://bronze")
    # "iceberg" en production, "parquet" pour les tests locaux
    storage: str = os.getenv("SILVER_STORAGE", "iceberg")
    catalog: str = os.getenv("SILVER_CATALOG", "lake")
    namespace: str = os.getenv("SILVER_NAMESPACE", "silver")
    # Utilisé seulement en mode parquet
    local_root: str = os.getenv("SILVER_LOCAL_ROOT", "/tmp/silver")
    # Au-delà de ce taux de rejets, le job échoue (Livrable 1 : 5 %)
    reject_threshold: float = float(os.getenv("SILVER_REJECT_THRESHOLD", "0.05"))
    # Nom de la variable qui contient la clé HMAC (lue dans OpenBao par Airflow)
    hmac_key_env: str = "SILVER_HMAC_KEY"
    # Vérifie l'empreinte SHA-256 de chaque fichier contre le manifeste NiFi
    verify_manifest: bool = os.getenv("SILVER_VERIFY_MANIFEST", "1") != "0"


# ---------------------------------------------------------------------------
# Description des sources Bronze
#   path         : dossier de la source sous BRONZE_ROOT. Rangement NiFi du groupe :
#                  bronze/<source>/<AAAA-MM-JJ>/<fichier> + <fichier>.manifest.json
#   format       : auto (défaut : selon l'extension .parquet/.csv/.txt/.xlsx/.json)
#                  ou insee_fixe (fichier INSEE officiel à largeur fixe)
#   encoding     : encodage des fichiers plats (défaut UTF-8)
#   include      : regex sur le chemin relatif à la source ; les autres fichiers
#                  sont ignorés (plusieurs fichiers peuvent partager un dossier)
#   date_formats : formats de date à essayer en premier (Java DateTimeFormatter)
#   columns      : champ logique -> liste de noms candidats dans la source
# ---------------------------------------------------------------------------
SOURCES: dict[str, dict] = {
    # ---- Fichier COG INSEE (5e source, référentiel, à télécharger sur insee.fr) ----
    "cog_communes": {
        "path": "cog", "include": r"(?i)(^|/)v_commune_\d{4}\.csv$",
        "columns": {
            "typecom": ["TYPECOM"], "com": ["COM"], "reg": ["REG"], "dep": ["DEP"],
            "libelle": ["LIBELLE"], "comparent": ["COMPARENT"],
        },
    },
    "cog_departements": {
        "path": "cog", "include": r"(?i)(^|/)v_departement_\d{4}\.csv$",
        "columns": {"dep": ["DEP"], "reg": ["REG"], "libelle": ["LIBELLE"]},
    },
    "cog_regions": {
        "path": "cog", "include": r"(?i)(^|/)v_region_\d{4}\.csv$",
        "columns": {"reg": ["REG"], "libelle": ["LIBELLE"]},
    },
    # Facultatif : mouvements de communes (fusions), pour recoder les anciens codes
    "cog_mouvements": {
        "path": "cog", "include": r"(?i)(^|/)v_mvt_commune_\d{4}\.csv$", "optional": True,
        "columns": {"com_av": ["COM_AV"], "com_ap": ["COM_AP"], "date_eff": ["DATE_EFF"],
                    "typecom_ap": ["TYPECOM_AP"]},
    },

    # ---- Source 1 : base PostgreSQL (dump DATA2023), extraite par NiFi ----
    # Une table = un fichier (ou un dossier) dont le nom commence par le nom de la
    # table : s1_postgres/2026-11-02/patient.parquet, .../Consultation.csv, etc.
    # Format déduit de l'extension (Parquet ou CSV ; Avro : ajouter spark-avro).
    "s1_patient": {
        "path": "s1_postgres", "include": r"(?i)(^|/)patient([./_-]|$)",
        # Colonne "Date" de la table Patient au format américain : 4/6/1980, 7/25/2013
        "date_formats": ["M/d/yyyy"],
        "columns": {
            "id_patient": ["id_patient"],
            "sexe": ["sexe"],                       # female / male
            "date_naissance": ["date"],
            "code_postal": ["code_postal"],
        },
    },
    "s1_professionnel": {
        "path": "s1_postgres", "include": r"(?i)(^|/)professionnel_de_sante([./_-]|$)",
        "columns": {
            "identifiant": ["identifiant"],         # RPPS (11) ou ADELI (8-9)
            "type_identifiant": ["type_identifiant"],
            "civilite": ["civilite"],
            "categorie_professionnelle": ["categorie_professionnelle"],
            "profession": ["profession"],
            "code_specialite": ["code_specialite"],
        },
    },
    "s1_specialites": {
        "path": "s1_postgres", "include": r"(?i)(^|/)specialites([./_-]|$)",
        "columns": {"code_specialite": ["code_specialite"], "fonction": ["fonction"],
                    "libelle_specialite": ["specialite"]},
    },
    "s1_diagnostic": {
        "path": "s1_postgres", "include": r"(?i)(^|/)diagnostic([./_-]|$)",
        "columns": {"code_diag": ["code_diag"], "libelle": ["diagnostic"]},
    },
    "s1_consultation": {
        "path": "s1_postgres", "include": r"(?i)(^|/)consultation([./_-]|$)",
        "columns": {
            "id_consultation": ["num_consultation"],
            "id_patient": ["id_patient"],
            "id_professionnel": ["id_prof_sante"],
            "code_diag": ["code_diag"],
            "date": ["date"],
            "heure_debut": ["heure_debut"],
            "heure_fin": ["heure_fin"],
            "motif": ["motif"],
        },
    },
    "s1_prescription": {
        "path": "s1_postgres", "include": r"(?i)(^|/)prescription([./_-]|$)", "optional": True,
        "columns": {"id_consultation": ["num_consultation"]},
    },

    # ---- Fichiers du dossier "Etablissement de SANTE" (source 2) ----
    "s2_etablissements": {
        "path": "s2_etablissements", "include": r"(?i)etablissement_sante[^/]*\.csv$",
        "columns": {
            "id_organisation": ["identifiant_organisation"],
            # Presque toujours vide dans le fichier réel : le FINESS site est
            # déduit de identifiant_organisation "F<FINESS>" (voir le job).
            "finess_site": ["finess_site"],
            "finess_juridique": ["finess_etablissement_juridique"],
            "raison_sociale": ["raison_sociale_site", "raison_sociale"],
            "siret": ["siret_site", "siret"],
            "adresse_complete": ["adresse", "adresse_complete"],
            "code_postal": ["code_postal"],
            "code_commune_insee": ["code_commune"],
        },
    },
    # Lieu d'exercice : professionnel -> organisation (établissement d'une consultation)
    "s2_activite": {
        "path": "s2_etablissements", "include": r"(?i)activite_professionnel_sante[^/]*\.csv$", "optional": True,
        "columns": {
            "identifiant": ["identifiant"],
            "id_organisation": ["identifiant_organisation"],
        },
    },

    # ---- Hospitalisations : fichier CSV séparé (pas dans la base PostgreSQL) ----
    "s1_hospitalisation": {
        "path": "s1_hospitalisations", "include": r"(?i)hospitalisations?[^/]*\.csv$",
        "date_formats": ["dd/MM/yyyy"],
        "columns": {
            "id_hospitalisation": ["num_hospitalisation"],
            "id_patient": ["id_patient"],
            "id_organisation": ["identifiant_organisation"],
            "code_diag": ["code_diagnostic"],
            "date_entree": ["date_entree"],
            "date_sortie": ["date_sortie"],
            "duree_jours": ["jour_hospitalisation"],
        },
    },

    # ---- Source 3 : e-Satis (HAS) via FTP ----
    # Seuls les fichiers de résultats e-Satis sont lus : les autres indicateurs
    # IQSS du dossier (RCP, DPA, DAN, IPAQSS...) et les lexiques sont ignorés.
    # Année et type d'enquête lus dans le nom du fichier. CSV en Windows-1252,
    # résultats 2020 en .xlsx (lus avec openpyxl). Encodage ISO-8859-1 : Spark 4
    # refuse windows-1252, et les deux sont identiques pour les accents des régions.
    "s3_satisfaction": {
        "path": "s3_satisfaction", "encoding": "ISO-8859-1",
        "include": r"(?i)^(?!.*lexique)(.*/)?[^/]*esatis[^/]*\.(csv|xlsx)$",
        "columns": {
            # "finess" = FINESS JURIDIQUE ; le site est "finess_geo" -> en premier
            "finess_site": ["finess_geo", "finess_site"],
            "region_source": ["region"],
            "nb_reponses": ["nb_rep_score_all_rea_ajust", "nb_rep_score_all_ajust"],
            "score_global": ["score_all_rea_ajust", "score_all_ajust"],
            # e-Satis 48h MCO
            "score_accueil": ["score_accueil_rea_ajust", "score_acc_ajust"],
            "score_pec_infirmier": ["score_pecinf_rea_ajust"],
            "score_pec_medical": ["score_pecmed_rea_ajust"],
            "score_chambre": ["score_chambre_rea_ajust", "score_cer_ajust"],
            "score_repas": ["score_repas_rea_ajust"],
            "score_sortie": ["score_sortie_rea_ajust", "score_ovs_ajust"],
            # e-Satis CA : ACC = accueil, CER = chambre et repas/collation,
            # OVS = sortie ; PEC (prise en charge globale) et AVH (avant
            # l'hospitalisation) n'ont pas de colonne dans le contrat.
            "taux_recommandation": ["taux_reco_brut"],
            "classement": ["classement"],
            "evolution": ["evolution"],
        },
    },

    # ---- Source 4 : fichier des décès INSEE (deces.csv) ----
    # En-tête réel : nom,prenom,sexe,date_naissance,code_lieu_naissance,
    # lieu_naissance,pays_naissance,date_deces,code_lieu_deces,numero_acte_deces
    # Le format officiel INSEE à largeur fixe reste possible : format "insee_fixe".
    "s4_deces": {
        "path": "s4_deces", "include": r"(?i)deces[^/]*\.(csv|txt)$",
        "columns": {
            "sexe": ["sexe"],
            "date_naissance": ["date_naissance", "datenaiss"],
            "date_deces": ["date_deces", "datedeces"],
            "lieu_deces": ["code_lieu_deces", "lieudeces"],
            "numero_acte": ["numero_acte_deces", "actedeces"],
        },
    },
}

# Format officiel INSEE à largeur fixe (positions 1-indexées, longueur)
INSEE_FIXE = {
    "nom_prenoms": (1, 80),
    "sexe": (81, 1),
    "date_naissance": (82, 8),
    "lieu_naissance": (90, 5),
    "date_deces": (155, 8),
    "lieu_deces": (163, 5),
    "numero_acte": (168, 9),
}
