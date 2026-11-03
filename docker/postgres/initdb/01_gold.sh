# Crée la couche Gold et le staging dans chu_dw (scripts sql/01 à 07).
# Les scripts de chargement (08 et suivants) ne sont pas lancés ici.
for f in 01_schemas 02_dimensions 03_dim_temps 04_faits_v0 05_faits_gold 06_index 07_staging; do
    echo "Gold : $f.sql"
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q -f "/sql/$f.sql"
done
