# Crée la couche Gold, le staging et les vues d'indicateurs dans chu_dw (sql/01 à 07 et 12).
# Les scripts de chargement 08 à 10 ne sont pas lancés ici.
for f in 01_schemas 02_dimensions 03_dim_temps 04_faits_v0 05_faits_gold 06_index 07_staging 12_vues_indicateurs; do
    echo "Gold : $f.sql"
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q -f "/sql/$f.sql"
done
