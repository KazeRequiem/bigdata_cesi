-- Version pgAdmin (Query Tool, F5) : variables psql remplacées par les valeurs de requetes/parametres.sql.
-- Avant optimisation : remplacer la ligne SET par  SET search_path TO gold_v0, gold;
-- Temps détaillé et plan : ajouter EXPLAIN ANALYZE devant le SELECT.
SET search_path TO gold;

-- B7 : nombre de décès par région sur une année (2019)
SELECT g.code_region, g.libelle_region,
       sum(f.nb_deces) AS nb_deces
FROM fait_deces f
JOIN gold.dim_geographie g ON g.sk_geo = f.sk_geo_deces
WHERE f.sk_date_deces BETWEEN 2019 * 10000 + 101 AND 2019 * 10000 + 1231
GROUP BY g.code_region, g.libelle_region
ORDER BY nb_deces DESC;
