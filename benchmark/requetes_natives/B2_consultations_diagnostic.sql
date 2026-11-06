-- Version pgAdmin (Query Tool, F5) : variables psql remplacées par les valeurs de requetes/parametres.sql.
-- Avant optimisation : remplacer la ligne SET par  SET search_path TO gold_v0, gold;
-- Temps détaillé et plan : ajouter EXPLAIN ANALYZE devant le SELECT.
SET search_path TO gold;

-- B2 : taux de consultation pour le diagnostic X sur la période Y
-- taux = consultations avec ce diagnostic / toutes les consultations de la période x 100
SELECT d.code_diag, d.libelle_diagnostic,
       count(*) AS nb_consultations,
       round(100.0 * count(*) / (SELECT count(*) FROM fait_consultation
                                 WHERE sk_date BETWEEN 20190101 AND 20191231), 3) AS taux_pct
FROM fait_consultation f
JOIN gold.dim_diagnostic d ON d.sk_diagnostic = f.sk_diagnostic
WHERE d.code_diag = 'B170'
  AND f.sk_date BETWEEN 20190101 AND 20191231
GROUP BY d.code_diag, d.libelle_diagnostic;
