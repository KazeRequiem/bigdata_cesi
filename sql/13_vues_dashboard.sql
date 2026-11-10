-- Livrable 3 : objets de la couche Gold dédiés aux tableaux de bord Superset.
--
-- Pourquoi des vues MATÉRIALISÉES : les vues gold.v_* (12_vues_indicateurs.sql) refont les jointures
-- de l'étoile à chaque graphique (1 à 4 s chacune, 24,7 M de lignes pour les décès). Un tableau de bord
-- lance 6 à 10 graphiques à la fois : on pré-calcule donc les jointures et les agrégats une fois par
-- chargement (même principe que les agrégats Gold / vues matérialisées vus en boucle 3).
--   gold.mv_consultations      B1, B2 : consultations à plat, sans identifiant de professionnel
--   gold.mv_consultations_pro  B6     : consultations par professionnel (réservé au rôle Direction)
--   gold.mv_hospitalisations   B4, B5 : séjours à plat
--   gold.mv_taux_hospitalisation B3, B5 : patients pris en charge / hospitalisés par année, sexe, âge
--   gold.mv_deces              B7     : décès agrégés par année, département, sexe, âge (+ population)
--   gold.v_satisfaction        B8     : vue simple (5 000 lignes, pas besoin de matérialiser)
--   gold.ref_population_region        : populations légales INSEE 2019 (taux pour 100 000 habitants)
--
-- Conventions communes à tous les jeux de données (les filtres natifs de Superset ciblent une colonne
-- par son NOM, il faut donc les mêmes noms partout) :
--   annee, date_annee (1er janvier, colonne temporelle), region, code_carte (carte), sexe, tranche_age.
--
-- À lancer après 12 (au démarrage) puis, après CHAQUE chargement des faits, rafraîchir avec
--   docker compose exec postgres psql -U chu -d chu_dw -f /sql/14_rafraichir_vues_dashboard.sql
-- Relancer ce fichier (13) seulement si la définition d'une vue change (il supprime et recrée).
-- Le compte bi lit ces objets grâce aux droits par défaut posés par 11_droits.sql (schéma gold).

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------------------------------
-- Populations légales 2019 (INSEE, population municipale, en vigueur au 1er janvier 2022)
-- Source : https://www.insee.fr/fr/statistiques/6013844?sommaire=6011075 (lu le 09/11/2026)
-- Mayotte est absente de ce millésime (dernier recensement général : 2017). On prend sa population de
-- référence 2017 (256 518 hab., https://www.insee.fr/fr/statistiques/3291775?sommaire=2120838) : seul
-- chiffre officiel disponible, d'une autre année que les autres régions (à signaler en lisant son taux).
-- Contrôle : la somme des départements (table suivante, hors Mayotte) = la somme des régions = 66 988 403.
-- ---------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.ref_population_region (
    code_region      CHAR(2)  PRIMARY KEY,
    population_2019  INTEGER  CHECK (population_2019 > 0),
    source           TEXT     NOT NULL
);

INSERT INTO gold.ref_population_region (code_region, population_2019, source) VALUES
    ('01',   384239, 'INSEE, populations légales 2019'),
    ('02',   364508, 'INSEE, populations légales 2019'),
    ('03',   281678, 'INSEE, populations légales 2019'),
    ('04',   861210, 'INSEE, populations légales 2019'),
    ('06',   256518, 'INSEE, populations de référence 2017 (Mayotte, dernier recensement)'),
    ('11', 12262544, 'INSEE, populations légales 2019'),
    ('24',  2573180, 'INSEE, populations légales 2019'),
    ('27',  2805580, 'INSEE, populations légales 2019'),
    ('28',  3325032, 'INSEE, populations légales 2019'),
    ('32',  6004947, 'INSEE, populations légales 2019'),
    ('44',  5556219, 'INSEE, populations légales 2019'),
    ('52',  3806461, 'INSEE, populations légales 2019'),
    ('53',  3354854, 'INSEE, populations légales 2019'),
    ('75',  6010289, 'INSEE, populations légales 2019'),
    ('76',  5933185, 'INSEE, populations légales 2019'),
    ('84',  8042936, 'INSEE, populations légales 2019'),
    ('93',  5081101, 'INSEE, populations légales 2019'),
    ('94',   340440, 'INSEE, populations légales 2019')
ON CONFLICT (code_region) DO UPDATE
    SET population_2019 = EXCLUDED.population_2019, source = EXCLUDED.source;

-- Populations légales 2019 des départements (même millésime ; Mayotte : référence 2017)
-- Source : https://www.insee.fr/fr/statistiques/6013867?sommaire=6011075 (lu le 09/11/2026)
-- Sert au taux de mortalité de la carte des départements (une carte en nombre ne montre que la taille
-- de la population).
CREATE TABLE IF NOT EXISTS gold.ref_population_departement (
    code_departement  VARCHAR(3)  PRIMARY KEY,
    population_2019   INTEGER     NOT NULL CHECK (population_2019 > 0),
    source            TEXT        NOT NULL
);

INSERT INTO gold.ref_population_departement (code_departement, population_2019, source) VALUES
    ('01',   652432, 'INSEE, populations légales 2019'),
    ('02',   531345, 'INSEE, populations légales 2019'),
    ('03',   335975, 'INSEE, populations légales 2019'),
    ('04',   164308, 'INSEE, populations légales 2019'),
    ('05',   141220, 'INSEE, populations légales 2019'),
    ('06',  1094283, 'INSEE, populations légales 2019'),
    ('07',   328278, 'INSEE, populations légales 2019'),
    ('08',   270582, 'INSEE, populations légales 2019'),
    ('09',   153287, 'INSEE, populations légales 2019'),
    ('10',   310242, 'INSEE, populations légales 2019'),
    ('11',   374070, 'INSEE, populations légales 2019'),
    ('12',   279595, 'INSEE, populations légales 2019'),
    ('13',  2043110, 'INSEE, populations légales 2019'),
    ('14',   694905, 'INSEE, populations légales 2019'),
    ('15',   144692, 'INSEE, populations légales 2019'),
    ('16',   352015, 'INSEE, populations légales 2019'),
    ('17',   651358, 'INSEE, populations légales 2019'),
    ('18',   302306, 'INSEE, populations légales 2019'),
    ('19',   240073, 'INSEE, populations légales 2019'),
    ('2A',   158507, 'INSEE, populations légales 2019'),
    ('2B',   181933, 'INSEE, populations légales 2019'),
    ('21',   534124, 'INSEE, populations légales 2019'),
    ('22',   600582, 'INSEE, populations légales 2019'),
    ('23',   116617, 'INSEE, populations légales 2019'),
    ('24',   413223, 'INSEE, populations légales 2019'),
    ('25',   543974, 'INSEE, populations légales 2019'),
    ('26',   516762, 'INSEE, populations légales 2019'),
    ('27',   599507, 'INSEE, populations légales 2019'),
    ('28',   431575, 'INSEE, populations légales 2019'),
    ('29',   915090, 'INSEE, populations légales 2019'),
    ('30',   748437, 'INSEE, populations légales 2019'),
    ('31',  1400039, 'INSEE, populations légales 2019'),
    ('32',   191377, 'INSEE, populations légales 2019'),
    ('33',  1623749, 'INSEE, populations légales 2019'),
    ('34',  1175623, 'INSEE, populations légales 2019'),
    ('35',  1079498, 'INSEE, populations légales 2019'),
    ('36',   219316, 'INSEE, populations légales 2019'),
    ('37',   610079, 'INSEE, populations légales 2019'),
    ('38',  1271166, 'INSEE, populations légales 2019'),
    ('39',   259199, 'INSEE, populations légales 2019'),
    ('40',   413690, 'INSEE, populations légales 2019'),
    ('41',   329470, 'INSEE, populations légales 2019'),
    ('42',   765634, 'INSEE, populations légales 2019'),
    ('43',   227570, 'INSEE, populations légales 2019'),
    ('44',  1429272, 'INSEE, populations légales 2019'),
    ('45',   680434, 'INSEE, populations légales 2019'),
    ('46',   174094, 'INSEE, populations légales 2019'),
    ('47',   331271, 'INSEE, populations légales 2019'),
    ('48',    76604, 'INSEE, populations légales 2019'),
    ('49',   818273, 'INSEE, populations légales 2019'),
    ('50',   495045, 'INSEE, populations légales 2019'),
    ('51',   566855, 'INSEE, populations légales 2019'),
    ('52',   172512, 'INSEE, populations légales 2019'),
    ('53',   307062, 'INSEE, populations légales 2019'),
    ('54',   733760, 'INSEE, populations légales 2019'),
    ('55',   184083, 'INSEE, populations légales 2019'),
    ('56',   759684, 'INSEE, populations légales 2019'),
    ('57',  1046543, 'INSEE, populations légales 2019'),
    ('58',   204452, 'INSEE, populations légales 2019'),
    ('59',  2608346, 'INSEE, populations légales 2019'),
    ('60',   829419, 'INSEE, populations légales 2019'),
    ('61',   279942, 'INSEE, populations légales 2019'),
    ('62',  1465278, 'INSEE, populations légales 2019'),
    ('63',   662152, 'INSEE, populations légales 2019'),
    ('64',   682621, 'INSEE, populations légales 2019'),
    ('65',   229567, 'INSEE, populations légales 2019'),
    ('66',   479979, 'INSEE, populations légales 2019'),
    ('67',  1140057, 'INSEE, populations légales 2019'),
    ('68',   767086, 'INSEE, populations légales 2019'),
    ('69',  1875747, 'INSEE, populations légales 2019'),
    ('70',   235313, 'INSEE, populations légales 2019'),
    ('71',   551493, 'INSEE, populations légales 2019'),
    ('72',   566412, 'INSEE, populations légales 2019'),
    ('73',   436434, 'INSEE, populations légales 2019'),
    ('74',   826094, 'INSEE, populations légales 2019'),
    ('75',  2165423, 'INSEE, populations légales 2019'),
    ('76',  1255633, 'INSEE, populations légales 2019'),
    ('77',  1421197, 'INSEE, populations légales 2019'),
    ('78',  1448207, 'INSEE, populations légales 2019'),
    ('79',   374878, 'INSEE, populations légales 2019'),
    ('80',   570559, 'INSEE, populations légales 2019'),
    ('81',   389844, 'INSEE, populations légales 2019'),
    ('82',   260669, 'INSEE, populations légales 2019'),
    ('83',  1076711, 'INSEE, populations légales 2019'),
    ('84',   561469, 'INSEE, populations légales 2019'),
    ('85',   685442, 'INSEE, populations légales 2019'),
    ('86',   438435, 'INSEE, populations légales 2019'),
    ('87',   372359, 'INSEE, populations légales 2019'),
    ('88',   364499, 'INSEE, populations légales 2019'),
    ('89',   335707, 'INSEE, populations légales 2019'),
    ('90',   141318, 'INSEE, populations légales 2019'),
    ('91',  1301659, 'INSEE, populations légales 2019'),
    ('92',  1624357, 'INSEE, populations légales 2019'),
    ('93',  1644903, 'INSEE, populations légales 2019'),
    ('94',  1407124, 'INSEE, populations légales 2019'),
    ('95',  1249674, 'INSEE, populations légales 2019'),
    ('971',   384239, 'INSEE, populations légales 2019'),
    ('972',   364508, 'INSEE, populations légales 2019'),
    ('973',   281678, 'INSEE, populations légales 2019'),
    ('974',   861210, 'INSEE, populations légales 2019'),
    ('976',   256518, 'INSEE, populations de référence 2017 (Mayotte, dernier recensement)')
ON CONFLICT (code_departement) DO UPDATE
    SET population_2019 = EXCLUDED.population_2019, source = EXCLUDED.source;

-- Codes attendus par la carte « Country Map » de Superset 4.1 (fond « france_regions »).
-- Son fichier GeoJSON n'utilise pas les codes ISO 3166-2 actuels de gold.ref_region_iso.code_iso pour
-- 6 régions (Corse = FR-COR au lieu de FR-20R, outre-mer = FR-GUA, FR-MTQ, FR-GUF, FR-LRE, FR-MAY) et
-- l'Île-de-France y est écrite « FR-IDF » SUIVI D'UNE TABULATION (défaut du fichier livré avec Superset) :
-- sans ce code exact, la région reste grise sur la carte.
ALTER TABLE gold.ref_region_iso ADD COLUMN IF NOT EXISTS code_carte VARCHAR(8);
UPDATE gold.ref_region_iso SET code_carte = CASE code_region
        WHEN '01' THEN 'FR-GUA' WHEN '02' THEN 'FR-MTQ' WHEN '03' THEN 'FR-GUF'
        WHEN '04' THEN 'FR-LRE' WHEN '06' THEN 'FR-MAY' WHEN '94' THEN 'FR-COR'
        WHEN '11' THEN 'FR-IDF' || chr(9)
        ELSE code_iso END;

-- Même chose pour le fond « france » (départements) : FR-<département>, sauf l'outre-mer.
CREATE OR REPLACE FUNCTION gold.code_carte_departement(dep TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE dep WHEN '971' THEN 'FR-GP' WHEN '972' THEN 'FR-MQ' WHEN '973' THEN 'FR-GF'
                    WHEN '974' THEN 'FR-RE' WHEN '976' THEN 'FR-YT' WHEN '00' THEN NULL
                    ELSE 'FR-' || dep END
$$;

-- Tranches d'âge communes (mêmes bornes que les requêtes B5 du benchmark) + tranches de 10 ans
-- pour les pyramides des âges.
CREATE OR REPLACE FUNCTION gold.tranche_age(age INTEGER) RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN age IS NULL THEN 'Inconnu'
                WHEN age < 18 THEN '0-17'
                WHEN age < 40 THEN '18-39'
                WHEN age < 65 THEN '40-64'
                WHEN age < 80 THEN '65-79'
                ELSE '80+' END
$$;

CREATE OR REPLACE FUNCTION gold.tranche_age_10(age INTEGER) RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE WHEN age IS NULL THEN 'Inconnu'
                WHEN age >= 90 THEN '90+'
                ELSE lpad((age / 10 * 10)::text, 2, '0') || '-' || lpad((age / 10 * 10 + 9)::text, 2, '0') END
$$;

CREATE OR REPLACE FUNCTION gold.libelle_sexe(s TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE s WHEN 'F' THEN 'Femme' WHEN 'M' THEN 'Homme' ELSE 'Indéterminé' END
$$;

DROP MATERIALIZED VIEW IF EXISTS gold.mv_consultations, gold.mv_consultations_pro, gold.mv_hospitalisations,
    gold.mv_taux_hospitalisation, gold.mv_deces CASCADE;
DROP VIEW IF EXISTS gold.v_satisfaction;

-- ---------------------------------------------------------------------------------------------------
-- B1, B2 : consultations à plat. Pas d'identifiant de professionnel ici (voir mv_consultations_pro) ;
-- sk_patient (clé technique, déjà pseudonymisée) sert à compter les patients distincts.
-- type_rattachement rappelle la limite de la source : 36 % des consultations n'ont pas d'établissement.
-- ---------------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold.mv_consultations AS
SELECT f.id_consultation,
       f.sk_patient,
       t.date                                         AS date_consultation,
       make_date(t.annee, 1, 1)                       AS date_annee,
       t.annee,
       t.mois,
       f.sk_etablissement,
       e.raison_sociale                               AS etablissement,
       e.finess_site,
       CASE WHEN f.sk_etablissement = -1 THEN 'Non rattaché'
            WHEN e.finess_site IS NOT NULL THEN 'Établissement FINESS'
            ELSE 'Autre structure (cabinet, centre)' END AS type_rattachement,
       g.code_region,
       g.libelle_region                               AS region,
       r.code_iso,
       r.code_carte,
       g.libelle_departement                          AS departement,
       d.code_diag,
       d.code_diag || ' - ' || d.libelle_diagnostic   AS diagnostic,
       coalesce(d.libelle_chapitre, 'Inconnu')        AS chapitre_diagnostic,
       coalesce(pr.libelle_specialite, 'Inconnue')    AS specialite,
       gold.libelle_sexe(p.sexe_patient)              AS sexe,
       f.age_consultation                             AS age,
       gold.tranche_age(f.age_consultation)           AS tranche_age,
       gold.tranche_age_10(f.age_consultation)        AS tranche_age_10,
       f.motif,
       f.duree_minutes,
       f.nb_prescriptions
FROM gold.fait_consultation f
JOIN gold.dim_temps         t  ON t.sk_date = f.sk_date
JOIN gold.dim_etablissement e  ON e.sk_etablissement = f.sk_etablissement
JOIN gold.dim_geographie    g  ON g.sk_geo = e.sk_geo
LEFT JOIN gold.ref_region_iso r ON r.code_region = g.code_region
JOIN gold.dim_diagnostic    d  ON d.sk_diagnostic = f.sk_diagnostic
JOIN gold.dim_professionnel pr ON pr.sk_professionnel = f.sk_professionnel
JOIN gold.dim_patient       p  ON p.sk_patient = f.sk_patient
WITH NO DATA;

CREATE UNIQUE INDEX ux_mv_consult_id ON gold.mv_consultations (id_consultation, date_consultation);
CREATE INDEX ix_mv_consult_annee ON gold.mv_consultations (annee);
-- recherches ciblées B1 (un établissement) et B2 (un diagnostic) sur une période : sans ces deux index
-- composites, la vue est lue en entier (mesure : 41 et 109 ms contre 10 ms sur les faits indexés)
CREATE INDEX ix_mv_consult_etab  ON gold.mv_consultations (sk_etablissement, date_consultation);
CREATE INDEX ix_mv_consult_diag  ON gold.mv_consultations (code_diag, date_consultation);
-- dénominateur des taux B1/B2 (toutes les consultations de la période)
CREATE INDEX ix_mv_consult_date  ON gold.mv_consultations (date_consultation);

-- ---------------------------------------------------------------------------------------------------
-- B6 : consultations par professionnel. Le n° RPPS/ADELI est déjà pseudonymisé en Silver (HMAC-SHA256,
-- plan de gouvernance du L1) ; code_professionnel en est une forme courte lisible à l'écran
-- (12 caractères hexadécimaux : risque de collision négligeable sur 1 M de professionnels).
-- Jeu de données séparé : seul le rôle Direction y a accès (l'activité individuelle d'un médecin est une
-- information de gestion sensible) ; les graphiques n'affichent que les professionnels ayant vu au moins
-- 10 patients distincts (seuil de confidentialité).
-- ---------------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold.mv_consultations_pro AS
SELECT f.id_consultation,
       f.sk_patient,
       make_date(t.annee, 1, 1)                     AS date_annee,
       t.annee,
       pr.id_professionnel,
       CASE WHEN pr.id_professionnel = 'INCONNU' THEN 'Inconnu'
            ELSE 'PS-' || upper(left(pr.id_professionnel, 12)) END AS code_professionnel,
       pr.type_identifiant,
       coalesce(pr.libelle_specialite, 'Inconnue')  AS specialite,
       e.raison_sociale                             AS etablissement,
       g.libelle_region                             AS region
FROM gold.fait_consultation f
JOIN gold.dim_temps         t  ON t.sk_date = f.sk_date
JOIN gold.dim_professionnel pr ON pr.sk_professionnel = f.sk_professionnel
JOIN gold.dim_etablissement e  ON e.sk_etablissement = f.sk_etablissement
JOIN gold.dim_geographie    g  ON g.sk_geo = e.sk_geo
WITH NO DATA;

CREATE UNIQUE INDEX ux_mv_consult_pro_id ON gold.mv_consultations_pro (id_consultation, annee);
CREATE INDEX ix_mv_consult_pro_annee ON gold.mv_consultations_pro (annee);

-- ---------------------------------------------------------------------------------------------------
-- B4, B5 : séjours hospitaliers à plat
-- ---------------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold.mv_hospitalisations AS
SELECT f.id_hospitalisation,
       f.sk_patient,
       t.date                                         AS date_entree,
       make_date(t.annee, 1, 1)                       AS date_annee,
       t.annee,
       t.mois,
       e.raison_sociale                               AS etablissement,
       g.libelle_region                               AS region,
       r.code_iso,
       r.code_carte,
       d.code_diag,
       d.code_diag || ' - ' || d.libelle_diagnostic   AS diagnostic,
       coalesce(d.libelle_chapitre, 'Inconnu')        AS chapitre_diagnostic,
       gold.libelle_sexe(p.sexe_patient)              AS sexe,
       f.age_hospitalisation                          AS age,
       gold.tranche_age(f.age_hospitalisation)        AS tranche_age,
       gold.tranche_age_10(f.age_hospitalisation)     AS tranche_age_10,
       f.duree_sejour_jours
FROM gold.fait_hospitalisation f
JOIN gold.dim_temps         t ON t.sk_date = f.sk_date_entree
JOIN gold.dim_etablissement e ON e.sk_etablissement = f.sk_etablissement
JOIN gold.dim_geographie    g ON g.sk_geo = e.sk_geo
LEFT JOIN gold.ref_region_iso r ON r.code_region = g.code_region
JOIN gold.dim_diagnostic    d ON d.sk_diagnostic = f.sk_diagnostic
JOIN gold.dim_patient       p ON p.sk_patient = f.sk_patient
WITH NO DATA;

CREATE UNIQUE INDEX ux_mv_hospit_id ON gold.mv_hospitalisations (id_hospitalisation);

-- ---------------------------------------------------------------------------------------------------
-- B3, B5 : taux d'hospitalisation = patients hospitalisés / patients pris en charge (au moins une
-- consultation ou un séjour dans l'année), même définition que gold.v_taux_hospitalisation, détaillée
-- par sexe et tranche d'âge (âge = année - année de naissance). Une ligne = un groupe de patients
-- disjoint : SUM(patients_hospitalises) / SUM(patients_pris_en_charge) reste juste quel que soit le
-- regroupement choisi dans Superset (sur plusieurs années, on compte des « patients-années »).
-- ---------------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold.mv_taux_hospitalisation AS
WITH hospitalises AS (
    SELECT t.annee, f.sk_patient, count(*) AS nb_sejours
    FROM gold.fait_hospitalisation f
    JOIN gold.dim_temps t ON t.sk_date = f.sk_date_entree
    WHERE f.sk_patient <> -1
    GROUP BY 1, 2
),
pris_en_charge AS (
    SELECT t.annee, f.sk_patient
    FROM gold.fait_consultation f
    JOIN gold.dim_temps t ON t.sk_date = f.sk_date
    WHERE f.sk_patient <> -1
    UNION
    SELECT annee, sk_patient FROM hospitalises
)
SELECT c.annee,
       make_date(c.annee, 1, 1)                                   AS date_annee,
       gold.libelle_sexe(p.sexe_patient)                          AS sexe,
       gold.tranche_age(c.annee - p.annee_naissance)              AS tranche_age,
       gold.tranche_age_10(c.annee - p.annee_naissance)           AS tranche_age_10,
       count(*)                                                   AS patients_pris_en_charge,
       count(h.sk_patient)                                        AS patients_hospitalises,
       coalesce(sum(h.nb_sejours), 0)                             AS nb_sejours
FROM pris_en_charge c
JOIN gold.dim_patient p ON p.sk_patient = c.sk_patient
LEFT JOIN hospitalises h ON h.annee = c.annee AND h.sk_patient = c.sk_patient
GROUP BY 1, 2, 3, 4, 5
WITH NO DATA;

-- ---------------------------------------------------------------------------------------------------
-- B7 : décès agrégés (24,7 M de lignes -> environ 125 000). Les populations de la région et du
-- département sont répétées sur chaque ligne : le taux pour 100 000 habitants se calcule AU NIVEAU
-- RÉGION avec 100000 * SUM(nb_deces) / MAX(population_region), AU NIVEAU DÉPARTEMENT avec
-- MAX(population_departement). somme_ages permet l'âge moyen au décès.
-- Décès comptés au LIEU du décès (pas au domicile) : à rappeler en lisant les cartes.
-- ---------------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold.mv_deces AS
SELECT t.annee,
       make_date(t.annee, 1, 1)                 AS date_annee,
       g.code_region,
       g.libelle_region                         AS region,
       r.code_iso,
       r.code_carte,
       g.code_departement,
       g.libelle_departement                    AS departement,
       gold.code_carte_departement(g.code_departement) AS code_carte_departement,
       gold.libelle_sexe(f.sexe_defunt)         AS sexe,
       gold.tranche_age(f.age_deces)            AS tranche_age,
       gold.tranche_age_10(f.age_deces)         AS tranche_age_10,
       pop.population_2019                      AS population_region,
       popd.population_2019                     AS population_departement,
       sum(f.nb_deces)                          AS nb_deces,
       sum(f.age_deces)                         AS somme_ages,
       count(f.age_deces)                       AS nb_deces_age_connu
FROM gold.fait_deces f
JOIN gold.dim_temps      t ON t.sk_date = f.sk_date_deces
JOIN gold.dim_geographie g ON g.sk_geo = f.sk_geo_deces
LEFT JOIN gold.ref_region_iso r          ON r.code_region = g.code_region
LEFT JOIN gold.ref_population_region pop ON pop.code_region = g.code_region
LEFT JOIN gold.ref_population_departement popd ON popd.code_departement = g.code_departement
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
WITH NO DATA;

CREATE INDEX ix_mv_deces_annee ON gold.mv_deces (annee);

-- ---------------------------------------------------------------------------------------------------
-- B8 : satisfaction par établissement (e-Satis). Vue simple : 5 000 lignes.
-- Moyenne pondérée par le nombre de répondants dans Superset :
--   SUM(score_global * nb_reponses) / SUM(nb_reponses)   (lignes sans score exclues ici)
-- ---------------------------------------------------------------------------------------------------
CREATE VIEW gold.v_satisfaction AS
SELECT s.id_satisfaction,
       t.annee,
       make_date(t.annee, 1, 1)                  AS date_annee,
       s.type_enquete,
       CASE s.type_enquete WHEN 'ESATIS48H_MCO' THEN 'Hospitalisation + 48 h (MCO)'
                           WHEN 'ESATISCA_MCO'  THEN 'Chirurgie ambulatoire'
                           ELSE s.type_enquete END AS enquete,
       e.raison_sociale                          AS etablissement,
       e.finess_site,
       g.code_region,
       g.libelle_region                          AS region,
       r.code_iso,
       r.code_carte,
       g.libelle_departement                     AS departement,
       s.nb_reponses,
       s.score_global,
       s.score_accueil,
       s.score_pec_infirmier,
       s.score_pec_medical,
       s.score_chambre,
       s.score_repas,
       s.score_sortie,
       s.taux_recommandation,
       s.classement
FROM gold.fait_satisfaction s
JOIN gold.dim_temps         t ON t.sk_date = s.sk_date
JOIN gold.dim_etablissement e ON e.sk_etablissement = s.sk_etablissement
JOIN gold.dim_geographie    g ON g.sk_geo = e.sk_geo
LEFT JOIN gold.ref_region_iso r ON r.code_region = g.code_region
WHERE s.score_global IS NOT NULL AND s.nb_reponses > 0;

-- Premier remplissage (les vues sont vides tant que les faits ne sont pas chargés : sans effet au
-- premier démarrage de PostgreSQL, utile quand on lance ce fichier à la main sur une base chargée).
\ir 14_rafraichir_vues_dashboard.sql
