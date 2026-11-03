"""Point d'entrée : exécute un ou plusieurs jobs Bronze -> Silver.

    spark-submit ... -m silver.run --job all
    python -m silver.run --job consultations --ingest-date 2026-11-02

Pour chaque job : lecture Bronze, transformation, quarantaine, contrôle du
seuil de rejets (5 %), écriture Silver (MERGE ou remplacement), journal.
Un job en échec arrête la chaîne (les suivants dépendent de lui).
"""
from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime

from pyspark.sql import functions as F

from . import contract
from .common import Quarantine, get_spark
from .config import Settings
from .context import Ctx
from .jobs import JOBS, ORDER
from .store import SilverStore


TECH_COLS = ["_id_deces", "_fichier", "_ingest_date"]


class RejectThresholdExceeded(RuntimeError):
    pass


def _conform(table: str, df):
    """Colonnes du contrat, typées, + colonnes techniques autorisées.
    Liste blanche volontaire : les colonnes de travail (_pid, _naissance...)
    contiennent des identifiants en clair et ne doivent jamais être écrites."""
    cols = [F.col(c).cast(contract.spark_type(t)).alias(c) for c, t in contract.CONTRACT[table][1]]
    tech = [c for c in TECH_COLS if c in df.columns]
    return df.select(*cols, *tech)


def run_job(ctx: Ctx, module) -> dict:
    table = module.TABLE
    started = datetime.now()
    q = Quarantine(table)
    stats = {"run_id": ctx.run_id, "table_silver": table, "debut": started, "ingest_date": ctx.ingest_date}
    try:
        out = module.build(ctx, q)
        df = _conform(table, out.df).withColumn("_run_id", F.lit(ctx.run_id)).cache()
        quarantine = q.frame(ctx.spark).withColumn("run_id", F.lit(ctx.run_id)) \
                                       .withColumn("horodatage", F.current_timestamp()).cache()

        written = df.count()
        by_cat = {r["categorie"]: r["n"] for r in quarantine.groupBy("categorie").agg(F.count("*").alias("n")).collect()}
        rejets = by_cat.get("rejet", 0)
        read = written + sum(by_cat.values())  # lignes lues hors doublons stricts fusionnés
        stats.update(lignes_lues=read, lignes_ecrites=written, rejets=rejets,
                     doublons=by_cat.get("doublon", 0), hors_perimetre=by_cat.get("hors_perimetre", 0))

        if quarantine.head(1):
            ctx.store.append("quarantaine", quarantine)
        ratio = rejets / read if read else 0.0
        if ratio > ctx.settings.reject_threshold:
            raise RejectThresholdExceeded(
                f"{table} : {rejets} rejets sur {read} lignes ({ratio:.1%}) > seuil "
                f"{ctx.settings.reject_threshold:.0%}. Silver non modifié, voir la quarantaine.")

        if module.MODE == "overwrite":
            ctx.store.overwrite(table, df)
        else:
            ctx.store.merge(table, df, contract.key(table))
        for name, extra in out.extras.items():
            ctx.store.overwrite(name, extra)
        stats.update(statut="OK", message=None)
        return stats
    except Exception as exc:  # journalisé puis relancé pour qu'Airflow voie l'échec
        stats.update(statut="ECHEC", message=str(exc)[:1000])
        raise
    finally:
        stats["fin"] = datetime.now()
        _journal(ctx, stats)


def _journal(ctx: Ctx, stats: dict) -> None:
    schema = ("run_id string, table_silver string, ingest_date string, debut timestamp, fin timestamp, "
              "lignes_lues long, lignes_ecrites long, rejets long, doublons long, hors_perimetre long, "
              "statut string, message string")
    row = {k: stats.get(k) for k in ["run_id", "table_silver", "ingest_date", "debut", "fin", "lignes_lues",
                                     "lignes_ecrites", "rejets", "doublons", "hors_perimetre", "statut", "message"]}
    ctx.store.append("journal_executions", ctx.spark.createDataFrame([row], schema))
    print(f"[silver] {row['table_silver']:<17} {row['statut']:<6} lues={row['lignes_lues']} "
          f"ecrites={row['lignes_ecrites']} rejets={row['rejets']} doublons={row['doublons']} "
          f"hors_perimetre={row['hors_perimetre']}" + (f" | {row['message']}" if row["message"] else ""))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Jobs Bronze -> Silver")
    p.add_argument("--job", default="all", help="nom de table, liste séparée par des virgules, ou 'all'")
    p.add_argument("--ingest-date", default=None, help="AAAA-MM-JJ ; par défaut toutes les ingestions")
    a = p.parse_args(argv)

    settings = Settings()
    spark = get_spark()
    ctx = Ctx(spark, settings, SilverStore(spark, settings),
              run_id=f"{datetime.now():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}", ingest_date=a.ingest_date)
    modules = ORDER if a.job == "all" else [JOBS[j.strip()] for j in a.job.split(",")]
    for m in modules:
        run_job(ctx, m)
    return 0


if __name__ == "__main__":
    sys.exit(main())
