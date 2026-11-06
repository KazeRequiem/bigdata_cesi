# bigdata_cesi

Projet Cloud Healthcare Unit : Lakehouse Bronze / Silver / Gold.

## Lancer le projet

```bash
cp .env.example .env                     # Silver + Gold
cp bronze/.env.example bronze/.env       # Bronze
docker compose up -d --build
```

`bronze/docker-compose.yml` est inclus dans le compose racine (même réseau) et garde son
propre `bronze/.env`. Les clés `S3_*` sont dans les deux fichiers : depuis la racine,
celles du `.env` racine priment (SeaweedFS et Spark ont donc toujours les mêmes) ; les garder
identiques pour pouvoir aussi lancer Bronze seul.

Au premier démarrage, PostgreSQL crée la base `iceberg_catalog`, exécute `sql/01` à `07`
dans `chu_dw` (couche Gold et staging), `sql/12_vues_indicateurs.sql`, puis `sql/11_droits.sql`
(comptes `etl` et `bi`). La base `superset` est aussi créée.
Pour recréer seulement Gold : `docker compose rm -sf postgres` puis `docker volume rm chu-lakehouse_pg_data`.
Jamais `docker compose down -v` : il efface aussi Bronze (fichiers SeaweedFS, flux NiFi, base source).

Superset : http://localhost:8088 (identifiants `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD`).
La connexion « CHU Gold » (compte `bi`, lecture seule) est déclarée au démarrage ; les vues
d'indicateurs `gold.v_*` servent de jeux de données.

pgAdmin : http://localhost:5050 (identifiants `PGADMIN_EMAIL` / `PGADMIN_PASSWORD` du `.env`).
Le serveur « CHU - chu_dw » est déjà déclaré ; mot de passe demandé : `POSTGRES_PASSWORD`.

OpenBao (coffre de secrets, mode développement) : http://localhost:8200, jeton `OPENBAO_ROOT_TOKEN` du `.env`.
Au démarrage, `openbao-init` y range la clé de pseudonymisation, les clés S3 et les comptes PostgreSQL du `.env` ;
le lanceur `silver` les lit dans le coffre à chaque job : le conteneur `spark` ne reçoit plus aucun secret
dans son environnement. Détails, limites et commandes : `docker/openbao/README.md`.

Airflow (orchestration) : http://localhost:8080 (`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`). Le DAG `chu_pipeline`
attend les manifestes Bronze puis enchaîne Silver, export, chargement Gold et rapport de qualité, avec reprises et
alerte : voir `docker/airflow/README.md`. Il remplace les commandes manuelles de la section « Puis Silver » ci-dessous.

Vérifications :

```bash
# tests Silver (jeu fictif, environ 5 minutes)
docker compose exec spark python3 -m pytest -q -p no:cacheprovider tests

# tables Iceberg de Silver
docker compose exec spark silver ddl

# tables Gold
docker compose exec postgres psql -U chu -d chu_dw -c "\dt gold.*"
```

Bronze, première fois : suivre `bronze/LISEZMOI.txt` (pilote JDBC dans `bronze/nifi-drivers`,
bucket `bronze`, restauration de la base source, import des flux NiFi). Les commandes
`docker compose exec seaweedfs|pg-source ...` se lancent maintenant depuis la racine.

Puis Silver lit `s3a://bronze` et la suite s'enchaîne :

```bash
docker compose exec spark silver run --job all
docker compose exec spark silver export --tables all

docker compose exec postgres psql -U chu -d chu_dw -f /sql/08_charger_dimensions.sql
docker compose exec postgres psql -U chu -d chu_dw -f /sql/09_charger_faits.sql
docker compose exec postgres psql -U chu -d chu_dw -f /sql/10_copier_v0.sql
```

## Benchmark

Les requêtes B1 à B8 sont dans `benchmark/requetes/`, leurs paramètres dans `benchmark/requetes/parametres.sql`.
Les faits n'ont pas de nom de schéma : `SET search_path TO gold_v0, gold;` mesure la version « avant »,
`SET search_path TO gold;` la version « après ».
