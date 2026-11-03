-- Tables de transit (schéma staging), alimentées par l'export Spark de Silver
-- (silver/src/silver/export_staging.py). Colonnes et types identiques à
-- silver/src/silver/contract.py.
-- Pas de contraintes : les données sont contrôlées en Silver, et l'export
-- vide puis recharge chaque table à chaque exécution (TRUNCATE + INSERT).
-- Le chargement Gold lit ces tables puis remplace les codes métier par les sk_*.

CREATE TABLE IF NOT EXISTS staging.communes (
    code_commune_insee   CHAR(5),
    libelle_commune      VARCHAR(100),
    code_departement     VARCHAR(3),
    libelle_departement  VARCHAR(60),
    code_region          CHAR(2),
    libelle_region       VARCHAR(60)
);

-- Toutes les versions de chaque établissement (une par date_extraction), pour le SCD2
CREATE TABLE IF NOT EXISTS staging.etablissements (
    id_organisation      VARCHAR(20),
    finess_site          CHAR(9),
    finess_juridique     CHAR(9),
    raison_sociale       VARCHAR(255),
    siret                CHAR(14),
    adresse_complete     VARCHAR(255),
    code_postal          CHAR(5),
    code_commune_insee   CHAR(5),
    date_extraction      DATE
);

CREATE TABLE IF NOT EXISTS staging.professionnels (
    id_professionnel           VARCHAR(64),
    type_identifiant           VARCHAR(5),
    civilite                   VARCHAR(8),
    categorie_professionnelle  VARCHAR(10),
    profession                 VARCHAR(50),
    code_specialite            VARCHAR(32),
    libelle_specialite         VARCHAR(60),
    fonction                   VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS staging.diagnostics (
    code_diag           VARCHAR(10),
    libelle_diagnostic  VARCHAR(255),
    code_chapitre       VARCHAR(5),
    libelle_chapitre    VARCHAR(150)
);

CREATE TABLE IF NOT EXISTS staging.patients (
    id_patient_pseudo  CHAR(64),
    sexe               CHAR(1),
    annee_naissance    SMALLINT,
    code_departement   VARCHAR(3)
);

CREATE TABLE IF NOT EXISTS staging.consultations (
    id_consultation    BIGINT,
    date_consultation  DATE,
    id_patient_pseudo  CHAR(64),
    id_professionnel   VARCHAR(64),
    code_diag          VARCHAR(10),
    id_organisation    VARCHAR(20),
    motif              VARCHAR(50),
    age_consultation   SMALLINT,
    duree_minutes      INTEGER,
    nb_prescriptions   SMALLINT
);

CREATE TABLE IF NOT EXISTS staging.hospitalisations (
    id_hospitalisation   BIGINT,
    date_entree          DATE,
    date_sortie          DATE,
    id_patient_pseudo    CHAR(64),
    code_diag            VARCHAR(10),
    id_organisation      VARCHAR(20),
    age_hospitalisation  SMALLINT,
    duree_sejour_jours   INTEGER
);

CREATE TABLE IF NOT EXISTS staging.satisfaction (
    finess_site          CHAR(9),
    annee                SMALLINT,
    type_enquete         VARCHAR(30),
    region_source        VARCHAR(60),
    nb_reponses          INTEGER,
    score_global         NUMERIC(5,2),
    score_accueil        NUMERIC(5,2),
    score_pec_infirmier  NUMERIC(5,2),
    score_pec_medical    NUMERIC(5,2),
    score_chambre        NUMERIC(5,2),
    score_repas          NUMERIC(5,2),
    score_sortie         NUMERIC(5,2),
    taux_recommandation  NUMERIC(5,2),
    classement           VARCHAR(2),
    evolution            VARCHAR(30)
);

-- Pas d'identifiant : la clé de dédoublonnage reste en Silver
CREATE TABLE IF NOT EXISTS staging.deces (
    date_deces          DATE,
    code_commune_deces  CHAR(5),
    sexe                CHAR(1),
    age_deces           SMALLINT
);
