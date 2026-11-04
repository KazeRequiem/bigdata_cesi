-- Version pgAdmin (Query Tool, F5) : variables psql remplacées par les valeurs de requetes/parametres.sql.
-- Avant optimisation : remplacer la ligne SET par  SET search_path TO gold_v0, gold;
-- Temps détaillé et plan : ajouter EXPLAIN ANALYZE devant le SELECT.
SET search_path TO gold;

-- B8 : taux global de satisfaction par région sur une année (2020)
-- score pondéré par le nombre de répondants de chaque établissement
SELECT g.code_region, g.libelle_region,
       count(DISTINCT s.sk_etablissement) AS nb_etablissements,
       sum(s.nb_reponses)                 AS nb_reponses,
       round(sum(s.score_global * s.nb_reponses) / NULLIF(sum(s.nb_reponses), 0), 2) AS score_pondere
FROM fait_satisfaction s
JOIN gold.dim_etablissement e ON e.sk_etablissement = s.sk_etablissement
JOIN gold.dim_geographie    g ON g.sk_geo = e.sk_geo
WHERE s.sk_date = 2020 * 10000 + 101
  AND s.score_global IS NOT NULL AND s.nb_reponses > 0
GROUP BY g.code_region, g.libelle_region
ORDER BY score_pondere DESC;
