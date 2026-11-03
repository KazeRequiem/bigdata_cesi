-- Copie des faits de gold vers gold_v0 (version "avant" du benchmark), après 09_charger_faits.sql :
--   docker compose exec postgres psql -U chu -d chu_dw -f /sql/10_copier_v0.sql
-- Les deux versions contiennent exactement les mêmes lignes : seule la structure physique change.

\set ON_ERROR_STOP on

BEGIN;
TRUNCATE gold_v0.fait_consultation, gold_v0.fait_hospitalisation,
         gold_v0.fait_satisfaction, gold_v0.fait_deces;
INSERT INTO gold_v0.fait_consultation    SELECT * FROM gold.fait_consultation;
INSERT INTO gold_v0.fait_hospitalisation SELECT * FROM gold.fait_hospitalisation;
INSERT INTO gold_v0.fait_satisfaction    SELECT * FROM gold.fait_satisfaction;
INSERT INTO gold_v0.fait_deces           SELECT * FROM gold.fait_deces;
COMMIT;

-- statistiques à jour des deux côtés avant toute mesure
VACUUM ANALYZE gold_v0.fait_consultation, gold_v0.fait_hospitalisation,
               gold_v0.fait_satisfaction, gold_v0.fait_deces,
               gold.fait_consultation, gold.fait_hospitalisation,
               gold.fait_satisfaction, gold.fait_deces;

SELECT 'fait_consultation' AS fait, (SELECT count(*) FROM gold.fait_consultation) AS gold,
       (SELECT count(*) FROM gold_v0.fait_consultation) AS gold_v0
UNION ALL SELECT 'fait_hospitalisation', (SELECT count(*) FROM gold.fait_hospitalisation),
       (SELECT count(*) FROM gold_v0.fait_hospitalisation)
UNION ALL SELECT 'fait_satisfaction', (SELECT count(*) FROM gold.fait_satisfaction),
       (SELECT count(*) FROM gold_v0.fait_satisfaction)
UNION ALL SELECT 'fait_deces', (SELECT count(*) FROM gold.fait_deces),
       (SELECT count(*) FROM gold_v0.fait_deces);
