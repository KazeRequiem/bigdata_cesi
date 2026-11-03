# bigdata_cesi

Projet Cloud Healthcare Unit : Lakehouse Bronze / Silver / Gold.

## Lancer Silver + Gold

```bash
cp .env.example .env              # puis changer les mots de passe
docker compose up -d --build
```

Au premier démarrage, PostgreSQL crée la base `iceberg_catalog`, exécute `sql/01` à `07`
dans `chu_dw` (couche Gold et staging) puis `sql/11_droits.sql` (comptes `etl` et `bi`).
Pour tout recréer : `docker compose down -v`.

pgAdmin : http://localhost:5050 (identifiants `PGADMIN_EMAIL` / `PGADMIN_PASSWORD` du `.env`).
Le serveur « CHU - chu_dw » est déjà déclaré ; mot de passe demandé : `POSTGRES_PASSWORD`.

Vérifications :

```bash
# tests Silver (jeu fictif, environ 5 minutes)
docker compose exec spark python3 -m pytest -q -p no:cacheprovider tests

# tables Iceberg de Silver
docker compose exec spark silver ddl

# tables Gold
docker compose exec postgres psql -U chu -d chu_dw -c "\dt gold.*"
```

Quand Bronze sera disponible :

```bash
docker compose exec spark silver run --job all
docker compose exec spark silver export --tables all

docker compose exec postgres psql -U chu -d chu_dw -f /sql/08_charger_dimensions.sql
docker compose exec postgres psql -U chu -d chu_dw -f /sql/09_charger_faits.sql
docker compose exec postgres psql -U chu -d chu_dw -f /sql/10_copier_v0.sql
```
