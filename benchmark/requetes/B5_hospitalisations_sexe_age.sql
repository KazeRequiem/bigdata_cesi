-- B5 : taux d'hospitalisation par sexe et par tranche d'âge sur la période Y
-- taux = séjours de la catégorie / tous les séjours de la période x 100
SELECT p.sexe_patient,
       CASE WHEN f.age_hospitalisation IS NULL THEN 'Inconnu'
            WHEN f.age_hospitalisation < 18 THEN '0-17'
            WHEN f.age_hospitalisation < 40 THEN '18-39'
            WHEN f.age_hospitalisation < 65 THEN '40-64'
            WHEN f.age_hospitalisation < 80 THEN '65-79'
            ELSE '80+' END AS tranche_age,
       count(*) AS nb_sejours,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS taux_pct
FROM fait_hospitalisation f
JOIN gold.dim_patient p ON p.sk_patient = f.sk_patient
WHERE f.sk_date_entree BETWEEN :debut AND :fin
GROUP BY 1, 2
ORDER BY 1, 2;
