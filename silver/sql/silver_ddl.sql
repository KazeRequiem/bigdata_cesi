-- ===========================================================================
-- Couche Silver — DDL des tables Apache Iceberg (Spark SQL)
-- Catalogue : lake (configuré dans spark-defaults / spark-submit)
-- À exécuter une fois avant le premier chargement :
--     spark-sql -f sql/silver_ddl.sql      (ou python -m silver.ddl)
--
-- Types : Iceberg n'a ni CHAR/VARCHAR ni SMALLINT. On utilise STRING et INT ;
-- les longueurs et formats du contrat sont garantis par les jobs, et l'export
-- vers staging convertit vers les types PostgreSQL exacts.
--
-- Partitionnement (Livrable 1 : « partitionnement par mois de l'événement ») :
-- - faits datés : transformation months() sur la date de l'événement. Iceberg
--   gère le partitionnement caché : une requête filtrée sur la date élague les
--   partitions sans que l'utilisateur connaisse la colonne de partition.
-- - satisfaction : par année (une publication par an, faible volume) ;
-- - établissements : par date d'extraction (une partition par version du fichier) ;
-- - petits référentiels (communes, diagnostics, patients, professionnels) :
--   pas de partition, des milliers de lignes ne justifient pas de petits fichiers.
-- Colonnes techniques (_...) : traçabilité, jamais exportées vers staging.
-- ===========================================================================

CREATE NAMESPACE IF NOT EXISTS lake.silver;

CREATE TABLE IF NOT EXISTS lake.silver.communes (
    code_commune_insee   STRING  COMMENT 'Code INSEE 5 car., unique (2A/2B conservés)',
    libelle_commune      STRING,
    code_departement     STRING,
    libelle_departement  STRING,
    code_region          STRING,
    libelle_region       STRING,
    _fichier             STRING,
    _ingest_date         DATE,
    _run_id              STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.silver.passage_communes (
    code_ancien          STRING  COMMENT 'Ancien code (commune déléguée, fusionnée)',
    code_actuel          STRING  COMMENT 'Code de la commune actuelle'
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.silver.etablissements (
    id_organisation      STRING  COMMENT 'Obligatoire ; clé avec date_extraction',
    finess_site          STRING,
    finess_juridique     STRING,
    raison_sociale       STRING,
    siret                STRING,
    adresse_complete     STRING,
    code_postal          STRING,
    code_commune_insee   STRING,
    date_extraction      DATE    COMMENT 'Date du fichier, sert au SCD2 en Gold',
    _fichier             STRING,
    _ingest_date         DATE,
    _run_id              STRING
) USING iceberg
PARTITIONED BY (date_extraction)
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.silver.professionnels (
    id_professionnel           STRING COMMENT 'HMAC-SHA256 hex du RPPS/ADELI',
    type_identifiant           STRING,
    civilite                   STRING,
    categorie_professionnelle  STRING,
    profession                 STRING,
    code_specialite            STRING,
    libelle_specialite         STRING,
    fonction                   STRING,
    _fichier                   STRING,
    _ingest_date               DATE,
    _run_id                    STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.silver.diagnostics (
    code_diag            STRING COMMENT 'CIM-10 sans point',
    libelle_diagnostic   STRING,
    code_chapitre        STRING COMMENT 'I à XXII',
    libelle_chapitre     STRING,
    _fichier             STRING,
    _ingest_date         DATE,
    _run_id              STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.silver.patients (
    id_patient_pseudo    STRING COMMENT 'HMAC-SHA256 hex',
    sexe                 STRING COMMENT 'M / F / NULL',
    annee_naissance      INT,
    code_departement     STRING COMMENT 'Département de résidence',
    _fichier             STRING,
    _ingest_date         DATE,
    _run_id              STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.silver.consultations (
    id_consultation      BIGINT,
    date_consultation    DATE,
    id_patient_pseudo    STRING,
    id_professionnel     STRING,
    code_diag            STRING,
    id_organisation      STRING COMMENT 'Lieu d’exercice du professionnel ; NULL si introuvable',
    motif                STRING,
    age_consultation     INT,
    duree_minutes        INT,
    nb_prescriptions     INT,
    _fichier             STRING,
    _ingest_date         DATE,
    _run_id              STRING
) USING iceberg
PARTITIONED BY (months(date_consultation))
TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read');

CREATE TABLE IF NOT EXISTS lake.silver.hospitalisations (
    id_hospitalisation   BIGINT,
    date_entree          DATE,
    date_sortie          DATE,
    id_patient_pseudo    STRING,
    code_diag            STRING,
    id_organisation      STRING  COMMENT 'NULL si absent du référentiel établissements',
    age_hospitalisation  INT,
    duree_sejour_jours   INT,
    _fichier             STRING,
    _ingest_date         DATE,
    _run_id              STRING
) USING iceberg
PARTITIONED BY (months(date_entree))
TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read');

CREATE TABLE IF NOT EXISTS lake.silver.satisfaction (
    finess_site          STRING,
    annee                INT,
    type_enquete         STRING,
    region_source        STRING,
    nb_reponses          INT,
    score_global         DECIMAL(5,2),
    score_accueil        DECIMAL(5,2),
    score_pec_infirmier  DECIMAL(5,2),
    score_pec_medical    DECIMAL(5,2),
    score_chambre        DECIMAL(5,2),
    score_repas          DECIMAL(5,2),
    score_sortie         DECIMAL(5,2),
    taux_recommandation  DECIMAL(5,2),
    classement           STRING,
    evolution            STRING,
    _fichier             STRING,
    _ingest_date         DATE,
    _run_id              STRING
) USING iceberg
PARTITIONED BY (annee)
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.silver.deces (
    date_deces           DATE,
    code_commune_deces   STRING,
    sexe                 STRING,
    age_deces            INT,
    _id_deces            STRING  COMMENT 'HMAC(lieu|date|n° acte) : clé de dédoublonnage',
    _fichier             STRING,
    _ingest_date         DATE,
    _run_id              STRING
) USING iceberg
PARTITIONED BY (years(date_deces))
TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read');

-- 25 M lignes : sans ordre d'écriture, Iceberg garde un fichier ouvert par partition
-- et par tâche (fanout) et sature la mémoire. Trié, chaque tâche écrit un fichier à la fois.
ALTER TABLE lake.silver.deces WRITE ORDERED BY date_deces;

-- Lignes écartées : référence seulement (clé, fichier, ligne), pas de contenu brut
CREATE TABLE IF NOT EXISTS lake.silver.quarantaine (
    table_silver  STRING,
    categorie     STRING COMMENT 'rejet / doublon / hors_perimetre',
    motif         STRING,
    cle           STRING COMMENT 'Clé métier, pseudonymisée pour les personnes',
    fichier       STRING COMMENT 'Fichier Bronze d’origine',
    ligne         BIGINT,
    run_id        STRING,
    horodatage    TIMESTAMP
) USING iceberg
PARTITIONED BY (table_silver, days(horodatage))
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.silver.journal_executions (
    run_id          STRING,
    table_silver    STRING,
    ingest_date     STRING,
    debut           TIMESTAMP,
    fin             TIMESTAMP,
    lignes_lues     BIGINT,
    lignes_ecrites  BIGINT,
    rejets          BIGINT,
    doublons        BIGINT,
    hors_perimetre  BIGINT,
    statut          STRING,
    message         STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2');
