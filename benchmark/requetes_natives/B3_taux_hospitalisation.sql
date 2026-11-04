-- Version pgAdmin (Query Tool, F5) : variables psql remplacées par les valeurs de requetes/parametres.sql.
-- Avant optimisation : remplacer la ligne SET par  SET search_path TO gold_v0, gold;
-- Temps détaillé et plan : ajouter EXPLAIN ANALYZE devant le SELECT.
SET search_path TO gold;

-- B3 : taux global d'hospitalisation sur la période Y
-- taux = patients hospitalisés / patients pris en charge (consultation ou hospitalisation) x 100
WITH hospitalises AS (
    SELECT DISTINCT sk_patient FROM fait_hospitalisation
    WHERE sk_date_entree BETWEEN 20190101 AND 20191231 AND sk_patient <> -1
),
pris_en_charge AS (
    SELECT sk_patient FROM fait_consultation
    WHERE sk_date BETWEEN 20190101 AND 20191231 AND sk_patient <> -1
    UNION
    SELECT sk_patient FROM hospitalises
)
SELECT (SELECT count(*) FROM hospitalises)   AS patients_hospitalises,
       (SELECT count(*) FROM pris_en_charge) AS patients_pris_en_charge,
       round(100.0 * (SELECT count(*) FROM hospitalises)
             / NULLIF((SELECT count(*) FROM pris_en_charge), 0), 2) AS taux_pct;
