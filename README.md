# bigdata_cesi

Projet Cloud Healthcare Unit : Lakehouse Bronze / Silver / Gold.

## Lancer Silver + Gold

```bash
cp .env.example .env              # puis changer les mots de passe
docker compose up -d --build
```

Au premier démarrage, PostgreSQL crée la base `iceberg_catalog` et exécute `sql/01` à `07`
dans `chu_dw` (couche Gold et staging). Pour tout recréer : `docker compose down -v`.

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
```
