-- Remplissage du calendrier : une ligne par jour de 1970 à 2030
-- (le fichier des décès INSEE commence en 1970).

INSERT INTO gold.dim_temps
    (sk_date, date, jour, jour_semaine, libelle_jour, semaine_iso,
     mois, libelle_mois, trimestre, semestre, annee, est_weekend)
SELECT
    to_char(d, 'YYYYMMDD')::INTEGER,
    d::DATE,
    EXTRACT(DAY FROM d)::SMALLINT,
    EXTRACT(ISODOW FROM d)::SMALLINT,
    (ARRAY['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche'])
        [EXTRACT(ISODOW FROM d)::INT],
    EXTRACT(WEEK FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    (ARRAY['Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin', 'Juillet',
           'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre'])
        [EXTRACT(MONTH FROM d)::INT],
    EXTRACT(QUARTER FROM d)::SMALLINT,
    CASE WHEN EXTRACT(MONTH FROM d) <= 6 THEN 1 ELSE 2 END,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series(DATE '1970-01-01', DATE '2030-12-31', INTERVAL '1 day') AS g(d)
ON CONFLICT (sk_date) DO NOTHING;
