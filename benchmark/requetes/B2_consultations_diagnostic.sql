-- B2 : taux de consultation pour le diagnostic X sur la période Y
-- taux = consultations avec ce diagnostic / toutes les consultations de la période x 100
SELECT d.code_diag, d.libelle_diagnostic,
       count(*) AS nb_consultations,
       round(100.0 * count(*) / (SELECT count(*) FROM fait_consultation
                                 WHERE sk_date BETWEEN :debut AND :fin), 3) AS taux_pct
FROM fait_consultation f
JOIN gold.dim_diagnostic d ON d.sk_diagnostic = f.sk_diagnostic
WHERE d.code_diag = :'code_diag'
  AND f.sk_date BETWEEN :debut AND :fin
GROUP BY d.code_diag, d.libelle_diagnostic;
