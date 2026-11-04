-- Version pgAdmin (Query Tool, F5) : variables psql remplacées par les valeurs de requetes/parametres.sql.
-- Avant optimisation : remplacer la ligne SET par  SET search_path TO gold_v0, gold;
-- Temps détaillé et plan : ajouter EXPLAIN ANALYZE devant le SELECT.
SET search_path TO gold;

-- B4 : taux d'hospitalisation par diagnostic sur la période Y
-- taux = séjours avec ce diagnostic / tous les séjours de la période x 100
SELECT d.code_diag, d.libelle_diagnostic,
       count(*) AS nb_sejours,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS taux_pct
FROM fait_hospitalisation f
JOIN gold.dim_diagnostic d ON d.sk_diagnostic = f.sk_diagnostic
WHERE f.sk_date_entree BETWEEN 20190101 AND 20191231
GROUP BY d.code_diag, d.libelle_diagnostic
ORDER BY nb_sejours DESC
LIMIT 20;
