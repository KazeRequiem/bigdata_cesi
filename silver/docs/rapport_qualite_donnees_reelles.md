> Rapport produit sur les **données réelles** du dossier DATA_2 (8 tables sur 9), rangées comme NiFi les dépose.
> Limites de ce passage : le fichier des décès n'était pas disponible, et le COG INSEE (absent de DATA_2)
> a été remplacé par une liste de communes tirée du fichier établissements, sans libellés de région.
> À régénérer sur la VM avec le vrai COG et deces.csv : `silver_cli.py qualite`.

# Rapport de qualité — couche Silver

Généré le 2026-11-02 22:13.

## 1. Complétude par attribut

Part des lignes où l'attribut est renseigné (non NULL).

### communes (12556 lignes)

| Attribut | Renseigné | Complétude |
|---|---:|---:|
| code_commune_insee | 12556 | 100.0 % |
| libelle_commune | 12556 | 100.0 % |
| code_departement | 12556 | 100.0 % |
| libelle_departement | 0 | 0.0 % |
| code_region | 0 | 0.0 % |
| libelle_region | 0 | 0.0 % |

### etablissements (416665 lignes)

| Attribut | Renseigné | Complétude |
|---|---:|---:|
| id_organisation | 416665 | 100.0 % |
| finess_site | 57692 | 13.8 % |
| finess_juridique | 57692 | 13.8 % |
| raison_sociale | 416640 | 100.0 % |
| siret | 74352 | 17.8 % |
| adresse_complete | 416662 | 100.0 % |
| code_postal | 416656 | 100.0 % |
| code_commune_insee | 277860 | 66.7 % |
| date_extraction | 416665 | 100.0 % |

### professionnels (1048575 lignes)

| Attribut | Renseigné | Complétude |
|---|---:|---:|
| id_professionnel | 1048575 | 100.0 % |
| type_identifiant | 1048575 | 100.0 % |
| civilite | 1048571 | 100.0 % |
| categorie_professionnelle | 1048575 | 100.0 % |
| profession | 1048575 | 100.0 % |
| code_specialite | 1048575 | 100.0 % |
| libelle_specialite | 1048574 | 100.0 % |
| fonction | 1048574 | 100.0 % |

### diagnostics (15490 lignes)

| Attribut | Renseigné | Complétude |
|---|---:|---:|
| code_diag | 15490 | 100.0 % |
| libelle_diagnostic | 15490 | 100.0 % |
| code_chapitre | 15490 | 100.0 % |
| libelle_chapitre | 15490 | 100.0 % |

### patients (100000 lignes)

| Attribut | Renseigné | Complétude |
|---|---:|---:|
| id_patient_pseudo | 100000 | 100.0 % |
| sexe | 100000 | 100.0 % |
| annee_naissance | 100000 | 100.0 % |
| code_departement | 99820 | 99.8 % |

### consultations (1027157 lignes)

| Attribut | Renseigné | Complétude |
|---|---:|---:|
| id_consultation | 1027157 | 100.0 % |
| date_consultation | 1027157 | 100.0 % |
| id_patient_pseudo | 1027157 | 100.0 % |
| id_professionnel | 1027157 | 100.0 % |
| code_diag | 1027157 | 100.0 % |
| id_organisation | 656063 | 63.9 % |
| motif | 1027157 | 100.0 % |
| age_consultation | 1003012 | 97.6 % |
| duree_minutes | 1027157 | 100.0 % |
| nb_prescriptions | 1027157 | 100.0 % |

### hospitalisations (2479 lignes)

| Attribut | Renseigné | Complétude |
|---|---:|---:|
| id_hospitalisation | 2479 | 100.0 % |
| date_entree | 2479 | 100.0 % |
| date_sortie | 2479 | 100.0 % |
| id_patient_pseudo | 2479 | 100.0 % |
| code_diag | 2479 | 100.0 % |
| id_organisation | 2479 | 100.0 % |
| age_hospitalisation | 2417 | 97.5 % |
| duree_sejour_jours | 2479 | 100.0 % |

### satisfaction (5266 lignes)

| Attribut | Renseigné | Complétude |
|---|---:|---:|
| finess_site | 5266 | 100.0 % |
| annee | 5266 | 100.0 % |
| type_enquete | 5266 | 100.0 % |
| region_source | 5266 | 100.0 % |
| nb_reponses | 3861 | 73.3 % |
| score_global | 3861 | 73.3 % |
| score_accueil | 3861 | 73.3 % |
| score_pec_infirmier | 2292 | 43.5 % |
| score_pec_medical | 2292 | 43.5 % |
| score_chambre | 3861 | 73.3 % |
| score_repas | 2292 | 43.5 % |
| score_sortie | 3861 | 73.3 % |
| taux_recommandation | 3255 | 61.8 % |
| classement | 5266 | 100.0 % |
| evolution | 2761 | 52.4 % |

### deces

_Table absente._

## 2. Unicité des clés

| Table | Clé | Lignes | Clés en double |
|---|---|---:|---:|
| communes | code_commune_insee | 12556 | 0 |
| etablissements | id_organisation, date_extraction | 416665 | 0 |
| professionnels | id_professionnel | 1048575 | 0 |
| diagnostics | code_diag | 15490 | 0 |
| patients | id_patient_pseudo | 100000 | 0 |
| consultations | id_consultation | 1027157 | 0 |
| hospitalisations | id_hospitalisation | 2479 | 0 |
| satisfaction | finess_site, annee, type_enquete | 5266 | 0 |

## 3. Cohérence référentielle

Parmi les valeurs renseignées, part qui existe dans la table de référence. Les valeurs NULL (inconnues) sont comptées à part.

| Contrôle | Renseignées | Trouvées | Cohérence | NULL |
|---|---:|---:|---:|---:|
| organisation consultation -> établissement | 656063 | 656063 | 100.0 % | 371094 |
| organisation hospitalisation -> établissement | 2479 | 2479 | 100.0 % | 0 |
| FINESS satisfaction -> établissement | 5266 | 5266 | 100.0 % | 0 |
| CIM-10 consultation -> diagnostics | 1027157 | 1027157 | 100.0 % | 0 |
| CIM-10 hospitalisation -> diagnostics | 2479 | 2479 | 100.0 % | 0 |
| patient consultation -> patients | 1027157 | 1027157 | 100.0 % | 0 |
| patient hospitalisation -> patients | 2479 | 2479 | 100.0 % | 0 |
| professionnel consultation -> professionnels | 1027157 | 1027157 | 100.0 % | 0 |
| commune établissement -> COG | 277860 | 277860 | 100.0 % | 138805 |
| Format FINESS site (9 caractères) dans établissements | 57692 | - | 0 invalide(s) | 358973 |

## 4. Quarantaine (dernière exécution de chaque table)

| Table | Catégorie | Motif | Lignes |
|---|---|---|---:|
| satisfaction | hors_perimetre | FINESS introuvable dans le référentiel établissements | 69 |

## 5. Dernière exécution par table

| Table | Statut | Lues | Écrites | Rejets | Taux de rejet | Doublons | Hors périmètre |
|---|---|---:|---:|---:|---:|---:|---:|
| communes | OK | 12556 | 12556 | 0 | 0.0 % | 0 | 0 |
| consultations | OK | 1027157 | 1027157 | 0 | 0.0 % | 0 | 0 |
| diagnostics | OK | 15490 | 15490 | 0 | 0.0 % | 0 | 0 |
| etablissements | OK | 416665 | 416665 | 0 | 0.0 % | 0 | 0 |
| hospitalisations | OK | 2479 | 2479 | 0 | 0.0 % | 0 | 0 |
| patients | OK | 100000 | 100000 | 0 | 0.0 % | 0 | 0 |
| professionnels | OK | 1048575 | 1048575 | 0 | 0.0 % | 0 | 0 |
| satisfaction | OK | 5335 | 5266 | 0 | 0.0 % | 0 | 69 |
