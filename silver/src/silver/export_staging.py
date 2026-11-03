"""Copie Silver -> schéma staging de PostgreSQL (contrat Silver -> staging).

Rechargement complet à chaque exécution : TRUNCATE puis insertion. Les tables
staging sont créées par le collègue Gold avec les types exacts du contrat ;
truncate=true garde leur définition (pas de DROP/CREATE par Spark).
Seules les colonnes du contrat sortent : les colonnes techniques "_..." restent
en Silver.

    PG_URL=jdbc:postgresql://hote:5432/chu PG_USER=... PG_PASSWORD=... \
    spark-submit --packages org.postgresql:postgresql:42.7.4 -m silver.export_staging
"""
from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import functions as F

from . import contract
from .common import get_spark
from .config import Settings
from .store import SilverStore

PG_CAST = {"SMALLINT": "smallint", "INTEGER": "int", "BIGINT": "bigint", "DATE": "date", "BOOLEAN": "boolean"}


def staging_frame(store: SilverStore, table: str):
    df = store.read(table)
    if df is None:
        raise RuntimeError(f"Table Silver absente : {table}")
    cols = []
    for c, t in contract.CONTRACT[table][1]:
        u = t.upper()
        spark_t = PG_CAST.get(u) or ("string" if u.startswith(("CHAR", "VARCHAR")) else contract.spark_type(t))
        cols.append(F.col(c).cast(spark_t).alias(c))
    return df.select(*cols)


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--tables", default="all")
    p.add_argument("--schema", default=os.getenv("PG_SCHEMA", "staging"))
    a = p.parse_args(argv)

    spark, settings = get_spark("silver-export-staging"), Settings()
    store = SilverStore(spark, settings)
    tables = list(contract.CONTRACT) if a.tables == "all" else a.tables.split(",")
    props = {"user": os.environ["PG_USER"], "password": os.environ["PG_PASSWORD"],
             "driver": "org.postgresql.Driver", "truncate": "true", "batchsize": "10000",
             "stringtype": "unspecified"}  # laisse PostgreSQL convertir vers CHAR/VARCHAR
    for t in tables:
        df = staging_frame(store, t)
        df.write.jdbc(os.environ["PG_URL"], f"{a.schema}.{t}", mode="overwrite", properties=props)
        print(f"[export] {a.schema}.{t} rechargée")
    return 0


if __name__ == "__main__":
    sys.exit(main())
