# Crée les comptes etl (Spark) et bi (Superset) avec leurs droits (sql/11_droits.sql).
echo "Droits : sql/11_droits.sql"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q \
    -v etl_user="$ETL_USER" -v etl_password="$ETL_PASSWORD" \
    -v bi_user="$BI_USER" -v bi_password="$BI_PASSWORD" \
    -f /sql/11_droits.sql
