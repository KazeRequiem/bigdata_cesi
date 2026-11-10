# Crée la couche Gold, le staging, les vues d'indicateurs et les vues des tableaux de bord dans chu_dw
# (sql/01 à 07, 12 et 13 ; les vues matérialisées de 13 restent vides tant que les faits ne sont pas chargés).
# Les scripts de chargement 08 à 10 ne sont pas lancés ici.
for f in 01_schemas 02_dimensions 03_dim_temps 04_faits_v0 05_faits_gold 06_index 07_staging 12_vues_indicateurs 13_vues_dashboard; do
    echo "Gold : $f.sql"
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q -f "/sql/$f.sql"
done
