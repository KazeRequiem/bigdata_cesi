"""Contrat Silver -> staging, sous forme exécutable.

Une seule source de vérité : les jobs Silver produisent exactement ces colonnes
(plus quelques colonnes techniques préfixées par "_" qui ne sortent jamais), et
l'export vers le schéma staging de PostgreSQL ne sélectionne que celles-ci.

Types : type PostgreSQL attendu en staging. En Silver (Iceberg), CHAR/VARCHAR
deviennent STRING et SMALLINT devient INT (Iceberg n'a pas de SMALLINT) ;
les longueurs sont contrôlées par les jobs.

Version validée avec le Gold (contrat du 2 novembre) :
  - patients : département de résidence (la source n'a que code postal et ville) ;
  - faits rattachés aux établissements par id_organisation, pas par FINESS
    (64 % des consultations rattachées au lieu de 37 %) ;
  - décès : date partielle ramenée au 1er janvier, sans colonne date_partielle
    (indicateurs de mortalité annuels).
"""
from __future__ import annotations

# table -> (clé métier, colonnes [(nom, type_pg)])
CONTRACT: dict[str, tuple[list[str], list[tuple[str, str]]]] = {
    "communes": (["code_commune_insee"], [
        ("code_commune_insee", "CHAR(5)"),
        ("libelle_commune", "VARCHAR(100)"),
        ("code_departement", "VARCHAR(3)"),
        ("libelle_departement", "VARCHAR(60)"),
        ("code_region", "CHAR(2)"),
        ("libelle_region", "VARCHAR(60)"),
    ]),
    "etablissements": (["id_organisation", "date_extraction"], [
        ("id_organisation", "VARCHAR(20)"),
        ("finess_site", "CHAR(9)"),
        ("finess_juridique", "CHAR(9)"),
        ("raison_sociale", "VARCHAR(255)"),
        ("siret", "CHAR(14)"),
        ("adresse_complete", "VARCHAR(255)"),
        ("code_postal", "CHAR(5)"),
        ("code_commune_insee", "CHAR(5)"),
        ("date_extraction", "DATE"),
    ]),
    "professionnels": (["id_professionnel"], [
        ("id_professionnel", "VARCHAR(64)"),
        ("type_identifiant", "VARCHAR(5)"),
        ("civilite", "VARCHAR(8)"),
        ("categorie_professionnelle", "VARCHAR(10)"),
        ("profession", "VARCHAR(50)"),
        ("code_specialite", "VARCHAR(32)"),
        ("libelle_specialite", "VARCHAR(60)"),
        ("fonction", "VARCHAR(50)"),
    ]),
    "diagnostics": (["code_diag"], [
        ("code_diag", "VARCHAR(10)"),
        ("libelle_diagnostic", "VARCHAR(255)"),
        ("code_chapitre", "VARCHAR(5)"),
        ("libelle_chapitre", "VARCHAR(150)"),
    ]),
    "patients": (["id_patient_pseudo"], [
        ("id_patient_pseudo", "CHAR(64)"),
        ("sexe", "CHAR(1)"),
        ("annee_naissance", "SMALLINT"),
        ("code_departement", "VARCHAR(3)"),
    ]),
    "consultations": (["id_consultation"], [
        ("id_consultation", "BIGINT"),
        ("date_consultation", "DATE"),
        ("id_patient_pseudo", "CHAR(64)"),
        ("id_professionnel", "VARCHAR(64)"),
        ("code_diag", "VARCHAR(10)"),
        ("id_organisation", "VARCHAR(20)"),
        ("motif", "VARCHAR(50)"),
        ("age_consultation", "SMALLINT"),
        ("duree_minutes", "INTEGER"),
        ("nb_prescriptions", "SMALLINT"),
    ]),
    "hospitalisations": (["id_hospitalisation"], [
        ("id_hospitalisation", "BIGINT"),
        ("date_entree", "DATE"),
        ("date_sortie", "DATE"),
        ("id_patient_pseudo", "CHAR(64)"),
        ("code_diag", "VARCHAR(10)"),
        ("id_organisation", "VARCHAR(20)"),
        ("age_hospitalisation", "SMALLINT"),
        ("duree_sejour_jours", "INTEGER"),
    ]),
    "satisfaction": (["finess_site", "annee", "type_enquete"], [
        ("finess_site", "CHAR(9)"),
        ("annee", "SMALLINT"),
        ("type_enquete", "VARCHAR(30)"),
        ("region_source", "VARCHAR(60)"),
        ("nb_reponses", "INTEGER"),
        ("score_global", "NUMERIC(5,2)"),
        ("score_accueil", "NUMERIC(5,2)"),
        ("score_pec_infirmier", "NUMERIC(5,2)"),
        ("score_pec_medical", "NUMERIC(5,2)"),
        ("score_chambre", "NUMERIC(5,2)"),
        ("score_repas", "NUMERIC(5,2)"),
        ("score_sortie", "NUMERIC(5,2)"),
        ("taux_recommandation", "NUMERIC(5,2)"),
        ("classement", "VARCHAR(2)"),
        ("evolution", "VARCHAR(30)"),
    ]),
    "deces": (["_id_deces"], [
        ("date_deces", "DATE"),
        ("code_commune_deces", "CHAR(5)"),
        ("sexe", "CHAR(1)"),
        ("age_deces", "SMALLINT"),
    ]),
}

# Types Spark utilisés en Silver pour chaque type PostgreSQL du contrat
def spark_type(pg_type: str) -> str:
    t = pg_type.upper()
    if t.startswith(("CHAR", "VARCHAR")):
        return "string"
    if t.startswith("NUMERIC"):
        return "decimal" + t[len("NUMERIC"):].lower()
    return {"SMALLINT": "int", "INTEGER": "int", "BIGINT": "bigint",
            "DATE": "date", "BOOLEAN": "boolean"}[t]


def columns(table: str) -> list[str]:
    return [c for c, _ in CONTRACT[table][1]]


def key(table: str) -> list[str]:
    return CONTRACT[table][0]
