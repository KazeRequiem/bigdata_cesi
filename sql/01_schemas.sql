-- Schémas de la base chu_dw
-- gold    : modèle en étoile optimisé (partitions + index)
-- gold_v0 : faits sans partitionnement ni index, pour le benchmark avant/après
-- staging : tables de transit chargées par Spark depuis Silver

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS gold_v0;
CREATE SCHEMA IF NOT EXISTS staging;
