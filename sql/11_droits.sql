-- Rôles et droits d'accès (moindre privilège).
--   etl : compte de Spark. Écrit uniquement dans staging, et possède le catalogue Iceberg.
--   bi  : compte de Superset. Lecture seule sur gold, aucun accès à staging ni à gold_v0.
-- Le compte administrateur (POSTGRES_USER) lance les scripts de chargement 08 à 10.
-- Noms et mots de passe passés en variables psql (lus dans le .env par 02_droits.sh) :
--   psql -v etl_user=... -v etl_password=... -v bi_user=... -v bi_password=... -f 11_droits.sql

\set ON_ERROR_STOP on

-- Création des comptes (ou mise à jour du mot de passe s'ils existent déjà)
SELECT format('CREATE ROLE %I LOGIN', :'etl_user')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'etl_user') \gexec
SELECT format('CREATE ROLE %I LOGIN', :'bi_user')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'bi_user') \gexec
SELECT format('ALTER ROLE %I PASSWORD %L', :'etl_user', :'etl_password') \gexec
SELECT format('ALTER ROLE %I PASSWORD %L', :'bi_user', :'bi_password') \gexec

-- Personne ne se connecte à la base sans y être autorisé
SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC', current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I, %I', current_database(), :'etl_user', :'bi_user') \gexec
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- ETL : staging seulement (l'export Spark vide puis remplit les tables)
SELECT format('GRANT USAGE ON SCHEMA staging TO %I', :'etl_user') \gexec
SELECT format('GRANT SELECT, INSERT, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA staging TO %I', :'etl_user') \gexec
SELECT format('ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT, INSERT, DELETE, TRUNCATE ON TABLES TO %I', :'etl_user') \gexec

-- ETL : propriétaire de la base du catalogue Iceberg (Spark y crée ses tables de métadonnées)
SELECT format('ALTER DATABASE iceberg_catalog OWNER TO %I', :'etl_user')
WHERE EXISTS (SELECT FROM pg_database WHERE datname = 'iceberg_catalog') \gexec

-- BI : lecture seule sur gold, y compris les tables créées plus tard
SELECT format('GRANT USAGE ON SCHEMA gold TO %I', :'bi_user') \gexec
SELECT format('GRANT SELECT ON ALL TABLES IN SCHEMA gold TO %I', :'bi_user') \gexec
SELECT format('ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO %I', :'bi_user') \gexec
