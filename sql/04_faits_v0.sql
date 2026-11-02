-- Tables de faits version "avant" (schéma gold_v0), utilisées pour le benchmark.
-- Pas de partitionnement, pas d'index en dehors des clés primaires.
-- Mêmes colonnes que les tables de 05_faits_gold.sql.

CREATE TABLE gold_v0.fait_consultation (
    id_consultation    BIGINT       NOT NULL PRIMARY KEY,
    sk_date            INTEGER      NOT NULL REFERENCES gold.dim_temps (sk_date),
    sk_patient         INTEGER      NOT NULL REFERENCES gold.dim_patient (sk_patient),
    sk_professionnel   INTEGER      NOT NULL REFERENCES gold.dim_professionnel (sk_professionnel),
    sk_diagnostic      INTEGER      NOT NULL REFERENCES gold.dim_diagnostic (sk_diagnostic),
    sk_etablissement   INTEGER      NOT NULL REFERENCES gold.dim_etablissement (sk_etablissement),
    motif              VARCHAR(50),
    age_consultation   SMALLINT     CHECK (age_consultation BETWEEN 0 AND 130),
    duree_minutes      INTEGER      CHECK (duree_minutes > 0),
    nb_prescriptions   SMALLINT     NOT NULL DEFAULT 0 CHECK (nb_prescriptions >= 0),
    nb_consultation    SMALLINT     NOT NULL DEFAULT 1 CHECK (nb_consultation = 1)
);

-- sk_date_sortie vide = séjour en cours
CREATE TABLE gold_v0.fait_hospitalisation (
    id_hospitalisation   BIGINT    NOT NULL PRIMARY KEY,
    sk_date_entree       INTEGER   NOT NULL REFERENCES gold.dim_temps (sk_date),
    sk_date_sortie       INTEGER            REFERENCES gold.dim_temps (sk_date),
    sk_patient           INTEGER   NOT NULL REFERENCES gold.dim_patient (sk_patient),
    sk_diagnostic        INTEGER   NOT NULL REFERENCES gold.dim_diagnostic (sk_diagnostic),
    sk_etablissement     INTEGER   NOT NULL REFERENCES gold.dim_etablissement (sk_etablissement),
    age_hospitalisation  SMALLINT  CHECK (age_hospitalisation BETWEEN 0 AND 130),
    duree_sejour_jours   INTEGER   NOT NULL CHECK (duree_sejour_jours >= 0),
    nb_hospitalisation   SMALLINT  NOT NULL DEFAULT 1 CHECK (nb_hospitalisation = 1),
    CONSTRAINT ck_hospit_dates CHECK (sk_date_sortie IS NULL OR sk_date_entree = -1
                                      OR sk_date_sortie >= sk_date_entree)
);

-- sk_date = 1er janvier de l'année de l'enquête
CREATE TABLE gold_v0.fait_satisfaction (
    id_satisfaction      BIGINT        NOT NULL PRIMARY KEY,
    sk_date              INTEGER       NOT NULL REFERENCES gold.dim_temps (sk_date),
    sk_etablissement     INTEGER       NOT NULL REFERENCES gold.dim_etablissement (sk_etablissement),
    type_enquete         VARCHAR(30)   NOT NULL,
    region_source        VARCHAR(60),
    nb_reponses          INTEGER       CHECK (nb_reponses >= 0),
    score_global         NUMERIC(5,2)  CHECK (score_global        BETWEEN 0 AND 100),
    score_accueil        NUMERIC(5,2)  CHECK (score_accueil       BETWEEN 0 AND 100),
    score_pec_infirmier  NUMERIC(5,2)  CHECK (score_pec_infirmier BETWEEN 0 AND 100),
    score_pec_medical    NUMERIC(5,2)  CHECK (score_pec_medical   BETWEEN 0 AND 100),
    score_chambre        NUMERIC(5,2)  CHECK (score_chambre       BETWEEN 0 AND 100),
    score_repas          NUMERIC(5,2)  CHECK (score_repas         BETWEEN 0 AND 100),
    score_sortie         NUMERIC(5,2)  CHECK (score_sortie        BETWEEN 0 AND 100),
    taux_recommandation  NUMERIC(5,2)  CHECK (taux_recommandation BETWEEN 0 AND 100),
    classement           VARCHAR(2)    CHECK (classement IN ('A', 'B', 'C', 'D', 'DI', 'NR', 'NV')),
    evolution            VARCHAR(30),
    CONSTRAINT ck_satisf_date CHECK (sk_date % 10000 = 101)
);

-- Pas de lien avec dim_patient (RGPD)
CREATE TABLE gold_v0.fait_deces (
    id_deces        BIGINT    NOT NULL PRIMARY KEY,
    sk_date_deces   INTEGER   NOT NULL REFERENCES gold.dim_temps (sk_date),
    sk_geo_deces    INTEGER   NOT NULL REFERENCES gold.dim_geographie (sk_geo),
    sexe_defunt     CHAR(1)   NOT NULL CHECK (sexe_defunt IN ('M', 'F')),
    age_deces       SMALLINT  CHECK (age_deces BETWEEN 0 AND 120),
    nb_deces        SMALLINT  NOT NULL DEFAULT 1 CHECK (nb_deces = 1)
);
