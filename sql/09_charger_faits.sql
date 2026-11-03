-- Chargement des faits depuis staging, après 08_charger_dimensions.sql :
--   docker compose exec postgres psql -U chu -d chu_dw -f /sql/09_charger_faits.sql
-- staging contient à chaque export la totalité des données Silver : chaque table de faits
-- est donc vidée puis rechargée (rejouable, sans doublon).
-- Les codes métier sont remplacés par les sk_* ; référence introuvable -> -1 ("Inconnu").
-- L'établissement retenu est la version valide à la date du fait (SCD2).

\set ON_ERROR_STOP on

-- Consultations
BEGIN;
TRUNCATE gold.fait_consultation;
INSERT INTO gold.fait_consultation
    (id_consultation, sk_date, sk_patient, sk_professionnel, sk_diagnostic, sk_etablissement,
     motif, age_consultation, duree_minutes, nb_prescriptions, nb_consultation)
SELECT c.id_consultation,
       COALESCE(t.sk_date, -1),
       COALESCE(p.sk_patient, -1),
       COALESCE(pr.sk_professionnel, -1),
       COALESCE(d.sk_diagnostic, -1),
       COALESCE(e.sk_etablissement, -1),          -- -1 = "Non rattaché"
       c.motif, c.age_consultation, c.duree_minutes,
       COALESCE(c.nb_prescriptions, 0),
       1
FROM staging.consultations c
LEFT JOIN gold.dim_temps         t  ON t.date = c.date_consultation
LEFT JOIN gold.dim_patient       p  ON p.id_patient_pseudo = c.id_patient_pseudo
LEFT JOIN gold.dim_professionnel pr ON pr.id_professionnel = c.id_professionnel
LEFT JOIN gold.dim_diagnostic    d  ON d.code_diag = c.code_diag
LEFT JOIN gold.dim_etablissement e  ON e.id_organisation = c.id_organisation
                                   AND c.date_consultation BETWEEN e.date_debut_validite AND e.date_fin_validite
WHERE c.id_consultation IS NOT NULL;
COMMIT;

-- Hospitalisations (date de sortie absente ou hors calendrier -> NULL)
BEGIN;
TRUNCATE gold.fait_hospitalisation;
INSERT INTO gold.fait_hospitalisation
    (id_hospitalisation, sk_date_entree, sk_date_sortie, sk_patient, sk_diagnostic, sk_etablissement,
     age_hospitalisation, duree_sejour_jours, nb_hospitalisation)
SELECT h.id_hospitalisation,
       COALESCE(te.sk_date, -1),
       ts.sk_date,
       COALESCE(p.sk_patient, -1),
       COALESCE(d.sk_diagnostic, -1),
       COALESCE(e.sk_etablissement, -1),
       h.age_hospitalisation, h.duree_sejour_jours, 1
FROM staging.hospitalisations h
LEFT JOIN gold.dim_temps         te ON te.date = h.date_entree
LEFT JOIN gold.dim_temps         ts ON ts.date = h.date_sortie
LEFT JOIN gold.dim_patient       p  ON p.id_patient_pseudo = h.id_patient_pseudo
LEFT JOIN gold.dim_diagnostic    d  ON d.code_diag = h.code_diag
LEFT JOIN gold.dim_etablissement e  ON e.id_organisation = h.id_organisation
                                   AND h.date_entree BETWEEN e.date_debut_validite AND e.date_fin_validite
WHERE h.id_hospitalisation IS NOT NULL;
COMMIT;

-- Satisfaction : rattachée par FINESS site, version de l'établissement valide au 1er janvier.
-- Plusieurs organisations peuvent avoir le même FINESS (ex. F + FINESS et F + FINESS + date) :
-- on garde l'identifiant le plus court (F + FINESS) pour ne pas compter
-- une enquête deux fois. Un résultat sans établissement n'est pas chargé.
BEGIN;
TRUNCATE gold.fait_satisfaction RESTART IDENTITY;
INSERT INTO gold.fait_satisfaction
    (sk_date, sk_etablissement, type_enquete, region_source, nb_reponses,
     score_global, score_accueil, score_pec_infirmier, score_pec_medical,
     score_chambre, score_repas, score_sortie, taux_recommandation, classement, evolution)
SELECT DISTINCT ON (s.finess_site, s.annee, s.type_enquete)
       s.annee * 10000 + 101,
       e.sk_etablissement,
       s.type_enquete, s.region_source, s.nb_reponses,
       s.score_global, s.score_accueil, s.score_pec_infirmier, s.score_pec_medical,
       s.score_chambre, s.score_repas, s.score_sortie, s.taux_recommandation, s.classement, s.evolution
FROM staging.satisfaction s
JOIN gold.dim_etablissement e ON e.finess_site = s.finess_site
                             AND make_date(s.annee, 1, 1) BETWEEN e.date_debut_validite AND e.date_fin_validite
WHERE s.annee BETWEEN 1970 AND 2030
ORDER BY s.finess_site, s.annee, s.type_enquete, length(e.id_organisation), e.id_organisation;
COMMIT;

-- Décès : sans identifiant stable, rechargement complet ; id_deces renuméroté
BEGIN;
TRUNCATE gold.fait_deces;
ALTER SEQUENCE gold.seq_fait_deces RESTART;
INSERT INTO gold.fait_deces (sk_date_deces, sk_geo_deces, sexe_defunt, age_deces, nb_deces)
SELECT COALESCE(t.sk_date, -1),
       COALESCE(g.sk_geo, -1),
       x.sexe, x.age_deces, 1
FROM staging.deces x
LEFT JOIN gold.dim_temps      t ON t.date = x.date_deces
LEFT JOIN gold.dim_geographie g ON g.code_commune_insee = x.code_commune_deces
WHERE x.date_deces IS NOT NULL
  AND x.sexe IN ('M', 'F');
COMMIT;

ANALYZE gold.fait_consultation, gold.fait_hospitalisation, gold.fait_satisfaction, gold.fait_deces;

-- Bilan : lignes en staging, lignes chargées, lignes rattachées à au moins un "Inconnu" (-1)
SELECT 'fait_consultation' AS fait,
       (SELECT count(*) FROM staging.consultations) AS staging,
       count(*) AS gold,
       count(*) FILTER (WHERE -1 IN (sk_date, sk_patient, sk_professionnel, sk_diagnostic)) AS avec_inconnu,
       count(*) FILTER (WHERE sk_etablissement = -1) AS etab_non_rattache
FROM gold.fait_consultation
UNION ALL
SELECT 'fait_hospitalisation', (SELECT count(*) FROM staging.hospitalisations), count(*),
       count(*) FILTER (WHERE -1 IN (sk_date_entree, sk_patient, sk_diagnostic)),
       count(*) FILTER (WHERE sk_etablissement = -1)
FROM gold.fait_hospitalisation
UNION ALL
SELECT 'fait_satisfaction', (SELECT count(*) FROM staging.satisfaction), count(*), 0, 0
FROM gold.fait_satisfaction
UNION ALL
SELECT 'fait_deces', (SELECT count(*) FROM staging.deces), count(*),
       count(*) FILTER (WHERE -1 IN (sk_date_deces, sk_geo_deces)), 0
FROM gold.fait_deces;
