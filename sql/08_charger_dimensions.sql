-- Chargement des dimensions depuis staging.
-- À lancer après l'export Silver -> staging, avant 09_charger_faits.sql :
--   docker compose exec postgres psql -U chu -d chu_dw -f /sql/08_charger_dimensions.sql
-- Rejouable : upsert sur la clé métier, aucune ligne en double.
-- dim_temps est remplie une fois pour toutes par 03_dim_temps.sql.

\set ON_ERROR_STOP on

BEGIN;

-- Géographie (SCD1 : la dernière valeur écrase la précédente)
INSERT INTO gold.dim_geographie
    (code_commune_insee, libelle_commune, code_departement, libelle_departement, code_region, libelle_region)
SELECT code_commune_insee, libelle_commune, code_departement, libelle_departement, code_region, libelle_region
FROM staging.communes
WHERE code_commune_insee IS NOT NULL
ON CONFLICT (code_commune_insee) DO UPDATE SET
    libelle_commune     = EXCLUDED.libelle_commune,
    code_departement    = EXCLUDED.code_departement,
    libelle_departement = EXCLUDED.libelle_departement,
    code_region         = EXCLUDED.code_region,
    libelle_region      = EXCLUDED.libelle_region;

-- Diagnostics
INSERT INTO gold.dim_diagnostic (code_diag, libelle_diagnostic, code_chapitre, libelle_chapitre)
SELECT code_diag, COALESCE(libelle_diagnostic, 'Libellé non renseigné'), code_chapitre, libelle_chapitre
FROM staging.diagnostics
WHERE code_diag IS NOT NULL
ON CONFLICT (code_diag) DO UPDATE SET
    libelle_diagnostic = EXCLUDED.libelle_diagnostic,
    code_chapitre      = EXCLUDED.code_chapitre,
    libelle_chapitre   = EXCLUDED.libelle_chapitre;

-- Professionnels
INSERT INTO gold.dim_professionnel
    (id_professionnel, type_identifiant, civilite, categorie_professionnelle,
     profession, code_specialite, libelle_specialite, fonction)
SELECT id_professionnel, type_identifiant, civilite, categorie_professionnelle,
       profession, code_specialite, libelle_specialite, fonction
FROM staging.professionnels
WHERE id_professionnel IS NOT NULL
ON CONFLICT (id_professionnel) DO UPDATE SET
    type_identifiant          = EXCLUDED.type_identifiant,
    civilite                  = EXCLUDED.civilite,
    categorie_professionnelle = EXCLUDED.categorie_professionnelle,
    profession                = EXCLUDED.profession,
    code_specialite           = EXCLUDED.code_specialite,
    libelle_specialite        = EXCLUDED.libelle_specialite,
    fonction                  = EXCLUDED.fonction;

-- Patients (sexe absent -> 'I')
INSERT INTO gold.dim_patient (id_patient_pseudo, sexe_patient, annee_naissance, code_departement)
SELECT id_patient_pseudo, COALESCE(sexe, 'I'), annee_naissance, code_departement
FROM staging.patients
WHERE id_patient_pseudo IS NOT NULL
ON CONFLICT (id_patient_pseudo) DO UPDATE SET
    sexe_patient     = EXCLUDED.sexe_patient,
    annee_naissance  = EXCLUDED.annee_naissance,
    code_departement = EXCLUDED.code_departement;

-- Établissements (SCD2)
-- staging.etablissements contient toutes les versions extraites (une par date_extraction).
-- 1. on ne garde que les extractions où un attribut a changé par rapport à la précédente ;
-- 2. chaque version est valable de sa date d'extraction à la veille de la suivante,
--    la première depuis 1900-01-01 (pour rattacher les faits anciens), la dernière jusqu'à 9999-12-31 ;
-- 3. upsert sur (id_organisation, date_debut_validite).
CREATE TEMP TABLE etab_versions ON COMMIT DROP AS
WITH src AS (
    SELECT s.id_organisation, s.finess_site, s.finess_juridique,
           COALESCE(s.raison_sociale, 'Raison sociale non renseignée') AS raison_sociale,
           s.siret, s.adresse_complete, s.code_postal,
           COALESCE(g.sk_geo, -1) AS sk_geo,
           s.date_extraction
    FROM staging.etablissements s
    LEFT JOIN gold.dim_geographie g ON g.code_commune_insee = s.code_commune_insee
    WHERE s.id_organisation IS NOT NULL AND s.date_extraction IS NOT NULL
),
compare AS (
    SELECT *,
           ROW(finess_site, finess_juridique, raison_sociale, siret, adresse_complete, code_postal, sk_geo)
             IS DISTINCT FROM
           LAG(ROW(finess_site, finess_juridique, raison_sociale, siret, adresse_complete, code_postal, sk_geo))
             OVER (PARTITION BY id_organisation ORDER BY date_extraction) AS a_change
    FROM src
),
versions AS (
    SELECT *, ROW_NUMBER() OVER w AS num_version, LEAD(date_extraction) OVER w AS debut_suivante
    FROM compare
    WHERE a_change
    WINDOW w AS (PARTITION BY id_organisation ORDER BY date_extraction)
)
SELECT id_organisation, finess_site, finess_juridique, raison_sociale, siret,
       adresse_complete, code_postal, sk_geo,
       CASE WHEN num_version = 1 THEN DATE '1900-01-01' ELSE date_extraction END AS date_debut_validite,
       COALESCE(debut_suivante - 1, DATE '9999-12-31')                          AS date_fin_validite,
       debut_suivante IS NULL                                                   AS est_courant
FROM versions;

-- versions déjà connues : mise à jour (la fin de validité change quand une version plus récente arrive)
UPDATE gold.dim_etablissement d
SET finess_site       = v.finess_site,
    finess_juridique  = v.finess_juridique,
    raison_sociale    = v.raison_sociale,
    siret             = v.siret,
    adresse_complete  = v.adresse_complete,
    code_postal       = v.code_postal,
    sk_geo            = v.sk_geo,
    date_fin_validite = v.date_fin_validite,
    est_courant       = v.est_courant
FROM etab_versions v
WHERE d.id_organisation = v.id_organisation
  AND d.date_debut_validite = v.date_debut_validite;

-- nouvelles versions
INSERT INTO gold.dim_etablissement
    (id_organisation, finess_site, finess_juridique, raison_sociale, siret, adresse_complete,
     code_postal, sk_geo, date_debut_validite, date_fin_validite, est_courant)
SELECT v.id_organisation, v.finess_site, v.finess_juridique, v.raison_sociale, v.siret, v.adresse_complete,
       v.code_postal, v.sk_geo, v.date_debut_validite, v.date_fin_validite, v.est_courant
FROM etab_versions v
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_etablissement d
                  WHERE d.id_organisation = v.id_organisation
                    AND d.date_debut_validite = v.date_debut_validite);

COMMIT;

ANALYZE gold.dim_geographie, gold.dim_diagnostic, gold.dim_professionnel,
        gold.dim_patient, gold.dim_etablissement;

SELECT 'dim_geographie' AS dimension, count(*) - 1 AS lignes FROM gold.dim_geographie
UNION ALL SELECT 'dim_diagnostic',    count(*) - 1 FROM gold.dim_diagnostic
UNION ALL SELECT 'dim_professionnel', count(*) - 1 FROM gold.dim_professionnel
UNION ALL SELECT 'dim_patient',       count(*) - 1 FROM gold.dim_patient
UNION ALL SELECT 'dim_etablissement', count(*) - 1 FROM gold.dim_etablissement;
