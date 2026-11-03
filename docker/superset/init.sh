#!/bin/bash
# Initialise Superset à chaque démarrage (sans effet si c'est déjà fait), puis lance le serveur.
set -e
superset db upgrade
superset fab create-admin \
    --username "$SUPERSET_ADMIN_USER" --password "$SUPERSET_ADMIN_PASSWORD" \
    --firstname Admin --lastname CHU --email "$SUPERSET_ADMIN_EMAIL" || true
superset init
# Connexion à la couche Gold avec le compte bi (lecture seule)
superset set-database-uri -d "CHU Gold" \
    -u "postgresql+psycopg2://${BI_USER}:${BI_PASSWORD}@postgres:5432/${POSTGRES_DB}" \
    || echo "Connexion CHU Gold à déclarer à la main dans Superset"
exec /usr/bin/run-server.sh
