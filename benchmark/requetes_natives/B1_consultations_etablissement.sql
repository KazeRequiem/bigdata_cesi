-- Version pgAdmin (Query Tool, F5) : variables psql remplacées par les valeurs de requetes/parametres.sql.
-- Avant optimisation : remplacer la ligne SET par  SET search_path TO gold_v0, gold;
-- Temps détaillé et plan : ajouter EXPLAIN ANALYZE devant le SELECT.
SET search_path TO gold;

-- B1 : taux de consultation dans l'établissement X sur la période Y
-- taux = consultations de X sur la période / toutes les consultations de la période x 100
-- Faits sans nom de schéma : le search_path choisit gold_v0 (avant) ou gold (après).
SELECT e.raison_sociale,
       count(*) AS nb_consultations,
       round(100.0 * count(*) / (SELECT count(*) FROM fait_consultation
                                 WHERE sk_date BETWEEN 20190101 AND 20191231), 3) AS taux_pct
FROM fait_consultation f
JOIN gold.dim_etablissement e ON e.sk_etablissement = f.sk_etablissement
WHERE e.id_organisation = 'F750821092'
  AND f.sk_date BETWEEN 20190101 AND 20191231
GROUP BY e.raison_sociale;
