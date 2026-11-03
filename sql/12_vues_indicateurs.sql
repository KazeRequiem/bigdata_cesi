-- Vues d'indicateurs pour Superset (Livrable 3), dans le schéma gold.
-- Chaque vue joint les faits à leurs dimensions : Superset filtre (année, établissement...)
-- et agrège lui-même. Les "taux" se calculent dans Superset en mode "contribution"
-- (part du total), sauf B3 et B8 qui ont leur propre calcul.
-- Le compte bi y a accès en lecture (droits par défaut posés par 11_droits.sql).
-- À lancer après 07 (au démarrage) ou à la main :
--   docker compose exec postgres psql -U chu -d chu_dw -f /sql/12_vues_indicateurs.sql

\set ON_ERROR_STOP on

-- Correspondance région INSEE -> code ISO 3166-2, utilisé par la carte de France de Superset
CREATE TABLE IF NOT EXISTS gold.ref_region_iso (
    code_region  CHAR(2)     PRIMARY KEY,
    code_iso     VARCHAR(6)  NOT NULL UNIQUE,
    libelle      VARCHAR(60) NOT NULL
);

INSERT INTO gold.ref_region_iso VALUES
    ('01', 'FR-971', 'Guadeloupe'),
    ('02', 'FR-972', 'Martinique'),
    ('03', 'FR-973', 'Guyane'),
    ('04', 'FR-974', 'La Réunion'),
    ('06', 'FR-976', 'Mayotte'),
    ('11', 'FR-IDF', 'Île-de-France'),
    ('24', 'FR-CVL', 'Centre-Val de Loire'),
    ('27', 'FR-BFC', 'Bourgogne-Franche-Comté'),
    ('28', 'FR-NOR', 'Normandie'),
    ('32', 'FR-HDF', 'Hauts-de-France'),
    ('44', 'FR-GES', 'Grand Est'),
    ('52', 'FR-PDL', 'Pays de la Loire'),
    ('53', 'FR-BRE', 'Bretagne'),
    ('75', 'FR-NAQ', 'Nouvelle-Aquitaine'),
    ('76', 'FR-OCC', 'Occitanie'),
    ('84', 'FR-ARA', 'Auvergne-Rhône-Alpes'),
    ('93', 'FR-PAC', 'Provence-Alpes-Côte d''Azur'),
    ('94', 'FR-20R', 'Corse')
ON CONFLICT (code_region) DO UPDATE SET code_iso = EXCLUDED.code_iso, libelle = EXCLUDED.libelle;

-- Consultations à plat : B1 (par établissement), B2 (par diagnostic), B6 (par professionnel)
CREATE OR REPLACE VIEW gold.v_consultations AS
SELECT f.id_consultation,
       t.date, t.annee, t.mois, t.libelle_mois,
       e.raison_sociale       AS etablissement,
       e.finess_site,
       ge.libelle_region      AS region_etablissement,
       d.code_diag, d.libelle_diagnostic, d.code_chapitre, d.libelle_chapitre,
       pr.profession, pr.libelle_specialite, pr.id_professionnel,
       p.sexe_patient,
       CASE WHEN f.age_consultation IS NULL THEN 'Inconnu'
            WHEN f.age_consultation < 18 THEN '0-17'
            WHEN f.age_consultation < 40 THEN '18-39'
            WHEN f.age_consultation < 65 THEN '40-64'
            WHEN f.age_consultation < 80 THEN '65-79'
            ELSE '80+' END    AS tranche_age,
       f.motif, f.duree_minutes, f.nb_prescriptions, f.nb_consultation
FROM gold.fait_consultation f
JOIN gold.dim_temps         t  ON t.sk_date = f.sk_date
JOIN gold.dim_etablissement e  ON e.sk_etablissement = f.sk_etablissement
JOIN gold.dim_geographie    ge ON ge.sk_geo = e.sk_geo
JOIN gold.dim_diagnostic    d  ON d.sk_diagnostic = f.sk_diagnostic
JOIN gold.dim_professionnel pr ON pr.sk_professionnel = f.sk_professionnel
JOIN gold.dim_patient       p  ON p.sk_patient = f.sk_patient;

-- Hospitalisations à plat : B4 (par diagnostic), B5 (par sexe et tranche d'âge)
CREATE OR REPLACE VIEW gold.v_hospitalisations AS
SELECT f.id_hospitalisation,
       t.date AS date_entree, t.annee, t.mois, t.libelle_mois,
       e.raison_sociale       AS etablissement,
       ge.libelle_region      AS region_etablissement,
       d.code_diag, d.libelle_diagnostic, d.code_chapitre, d.libelle_chapitre,
       p.sexe_patient,
       CASE WHEN f.age_hospitalisation IS NULL THEN 'Inconnu'
            WHEN f.age_hospitalisation < 18 THEN '0-17'
            WHEN f.age_hospitalisation < 40 THEN '18-39'
            WHEN f.age_hospitalisation < 65 THEN '40-64'
            WHEN f.age_hospitalisation < 80 THEN '65-79'
            ELSE '80+' END    AS tranche_age,
       f.duree_sejour_jours, f.nb_hospitalisation
FROM gold.fait_hospitalisation f
JOIN gold.dim_temps         t  ON t.sk_date = f.sk_date_entree
JOIN gold.dim_etablissement e  ON e.sk_etablissement = f.sk_etablissement
JOIN gold.dim_geographie    ge ON ge.sk_geo = e.sk_geo
JOIN gold.dim_diagnostic    d  ON d.sk_diagnostic = f.sk_diagnostic
JOIN gold.dim_patient       p  ON p.sk_patient = f.sk_patient;

-- B3 : taux global d'hospitalisation par année
-- = patients hospitalisés dans l'année / patients pris en charge dans l'année
--   (au moins une consultation ou une hospitalisation)
CREATE OR REPLACE VIEW gold.v_taux_hospitalisation AS
WITH hospitalises AS (
    SELECT DISTINCT t.annee, f.sk_patient
    FROM gold.fait_hospitalisation f
    JOIN gold.dim_temps t ON t.sk_date = f.sk_date_entree
    WHERE f.sk_patient <> -1
),
pris_en_charge AS (
    SELECT t.annee, f.sk_patient
    FROM gold.fait_consultation f
    JOIN gold.dim_temps t ON t.sk_date = f.sk_date
    WHERE f.sk_patient <> -1
    UNION
    SELECT annee, sk_patient FROM hospitalises
)
SELECT c.annee,
       count(h.sk_patient)                                         AS patients_hospitalises,
       count(*)                                                    AS patients_pris_en_charge,
       round(100.0 * count(h.sk_patient) / NULLIF(count(*), 0), 2) AS taux_hospitalisation_pct
FROM pris_en_charge c
LEFT JOIN hospitalises h ON h.annee = c.annee AND h.sk_patient = c.sk_patient
GROUP BY c.annee;

-- B7 : décès par région, année, sexe, tranche d'âge (déjà agrégé : la carte reste rapide)
CREATE OR REPLACE VIEW gold.v_deces_region AS
SELECT t.annee,
       g.code_region, g.libelle_region, r.code_iso,
       f.sexe_defunt,
       CASE WHEN f.age_deces IS NULL THEN 'Inconnu'
            WHEN f.age_deces < 18 THEN '0-17'
            WHEN f.age_deces < 40 THEN '18-39'
            WHEN f.age_deces < 65 THEN '40-64'
            WHEN f.age_deces < 80 THEN '65-79'
            ELSE '80+' END AS tranche_age,
       sum(f.nb_deces) AS nb_deces
FROM gold.fait_deces f
JOIN gold.dim_temps      t ON t.sk_date = f.sk_date_deces
JOIN gold.dim_geographie g ON g.sk_geo = f.sk_geo_deces
LEFT JOIN gold.ref_region_iso r ON r.code_region = g.code_region
GROUP BY 1, 2, 3, 4, 5, 6;

-- B8 : satisfaction par région et année
-- score pondéré = somme(score x répondants) / somme(répondants) ; dans Superset, utiliser
-- la métrique SUM(somme_ponderee) / SUM(nb_reponses) pour regrouper correctement.
CREATE OR REPLACE VIEW gold.v_satisfaction_region AS
SELECT t.annee, s.type_enquete,
       g.code_region, g.libelle_region, r.code_iso,
       count(DISTINCT s.sk_etablissement)          AS nb_etablissements,
       sum(s.nb_reponses)                          AS nb_reponses,
       sum(s.score_global * s.nb_reponses)         AS somme_ponderee,
       round(sum(s.score_global * s.nb_reponses) / NULLIF(sum(s.nb_reponses), 0), 2) AS score_global_pondere
FROM gold.fait_satisfaction s
JOIN gold.dim_temps         t ON t.sk_date = s.sk_date
JOIN gold.dim_etablissement e ON e.sk_etablissement = s.sk_etablissement
JOIN gold.dim_geographie    g ON g.sk_geo = e.sk_geo
LEFT JOIN gold.ref_region_iso r ON r.code_region = g.code_region
WHERE s.score_global IS NOT NULL AND s.nb_reponses > 0
GROUP BY 1, 2, 3, 4, 5;
