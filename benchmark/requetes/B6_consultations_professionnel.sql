-- B6 : taux de consultation par professionnel sur la période Y (les 20 plus consultés)
-- taux = consultations du professionnel / toutes les consultations de la période x 100
SELECT pr.id_professionnel, pr.profession, pr.libelle_specialite,
       count(*) AS nb_consultations,
       round(100.0 * count(*) / sum(count(*)) OVER (), 3) AS taux_pct
FROM fait_consultation f
JOIN gold.dim_professionnel pr ON pr.sk_professionnel = f.sk_professionnel
WHERE f.sk_date BETWEEN :debut AND :fin
GROUP BY pr.id_professionnel, pr.profession, pr.libelle_specialite
ORDER BY nb_consultations DESC
LIMIT 20;
