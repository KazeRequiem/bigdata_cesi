#!/bin/sh
# Initialise OpenBao (mode développement) : range les secrets du .env dans le coffre
# et crée un jeton de lecture limité pour la couche Silver.
# (Le journal d'audit est déclaré dans docker/openbao/audit.hcl, chargé au démarrage du serveur.)
# Lancé une fois par "docker compose up" (service openbao-init). Rejouable : les valeurs sont simplement réécrites.
set -eu

: "${BAO_ADDR:?BAO_ADDR manquant}"
: "${BAO_TOKEN:?BAO_TOKEN manquant (jeton racine, utilisé uniquement par ce script)}"

echo "[openbao-init] attente d'OpenBao ($BAO_ADDR)..."
i=0
until bao status >/dev/null 2>&1; do
  i=$((i + 1)); [ "$i" -gt 30 ] && { echo "[openbao-init] OpenBao ne répond pas" >&2; exit 1; }
  sleep 2
done

# Le mode développement monte déjà un coffre KV v2 sur secret/
bao kv put secret/chu/pseudonymisation cle="${SILVER_HMAC_KEY:?SILVER_HMAC_KEY manquant}" >/dev/null
bao kv put secret/chu/s3 access_key="${S3_ACCESS_KEY:?S3_ACCESS_KEY manquant}" secret_key="${S3_SECRET_KEY:?S3_SECRET_KEY manquant}" >/dev/null
bao kv put secret/chu/postgres_etl user="${ETL_USER:?ETL_USER manquant}" password="${ETL_PASSWORD:?ETL_PASSWORD manquant}" >/dev/null
bao kv put secret/chu/postgres_bi user="${BI_USER:?BI_USER manquant}" password="${BI_PASSWORD:?BI_PASSWORD manquant}" >/dev/null
bao kv put secret/chu/postgres_admin user="${POSTGRES_USER:?POSTGRES_USER manquant}" password="${POSTGRES_PASSWORD:?POSTGRES_PASSWORD manquant}" >/dev/null
# Accès SFTP : seulement si les variables sont définies dans le .env racine
if [ -n "${SFTP_USER:-}" ] && [ -n "${SFTP_PASSWORD:-}" ]; then
  bao kv put secret/chu/sftp user="$SFTP_USER" password="$SFTP_PASSWORD" >/dev/null
fi

# Politique de Silver : lecture seule des seuls secrets dont la couche a besoin
bao policy write silver - >/dev/null <<'POLICY'
path "secret/data/chu/pseudonymisation" { capabilities = ["read"] }
path "secret/data/chu/s3"               { capabilities = ["read"] }
path "secret/data/chu/postgres_etl"     { capabilities = ["read"] }
POLICY

# Jeton de Silver, déposé dans un volume que seul le conteneur spark monte (lecture seule)
mkdir -p /tokens
bao token create -policy=silver -orphan -period=720h -display-name=silver -field=token > /tokens/silver.token
chmod 644 /tokens/silver.token

echo "[openbao-init] secrets rangés :"
bao kv list secret/chu | sed 's/^/  - /'
echo "[openbao-init] politique 'silver' et jeton créés."
