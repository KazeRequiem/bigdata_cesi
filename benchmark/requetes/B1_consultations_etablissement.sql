-- B1 : taux de consultation dans l'établissement X sur la période Y
-- taux = consultations de X sur la période / toutes les consultations de la période x 100
-- Faits sans nom de schéma : le search_path choisit gold_v0 (avant) ou gold (après).
SELECT e.raison_sociale,
       count(*) AS nb_consultations,
       round(100.0 * count(*) / (SELECT count(*) FROM fait_consultation
                                 WHERE sk_date BETWEEN :debut AND :fin), 3) AS taux_pct
FROM fait_consultation f
JOIN gold.dim_etablissement e ON e.sk_etablissement = f.sk_etablissement
WHERE e.id_organisation = :'organisation'
  AND f.sk_date BETWEEN :debut AND :fin
GROUP BY e.raison_sociale;
