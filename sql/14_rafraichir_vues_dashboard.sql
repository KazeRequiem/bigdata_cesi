-- Rafraîchit les vues matérialisées des tableaux de bord (créées par 13_vues_dashboard.sql).
-- À lancer après chaque chargement des faits (09_charger_faits.sql) : tâche gold_vues_dashboard du DAG.
--   docker compose exec postgres psql -U chu -d chu_dw -f /sql/14_rafraichir_vues_dashboard.sql
-- Environ 1 min 30 sur les données complètes (dont l'essentiel pour mv_deces, 24,7 M de lignes lues).
-- Pas de CONCURRENTLY : plus lent, et les tableaux de bord ne sont pas consultés pendant le chargement.

\set ON_ERROR_STOP on
\timing on

REFRESH MATERIALIZED VIEW gold.mv_consultations;
REFRESH MATERIALIZED VIEW gold.mv_consultations_pro;
REFRESH MATERIALIZED VIEW gold.mv_hospitalisations;
REFRESH MATERIALIZED VIEW gold.mv_taux_hospitalisation;
REFRESH MATERIALIZED VIEW gold.mv_deces;

ANALYZE gold.mv_consultations, gold.mv_consultations_pro, gold.mv_hospitalisations,
        gold.mv_taux_hospitalisation, gold.mv_deces;
