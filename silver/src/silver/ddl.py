"""Exécute sql/silver_ddl.sql (création des tables Iceberg, idempotent).

    spark-submit ... -m silver.ddl [--file sql/silver_ddl.sql]
Le nom du catalogue "lake" est remplacé par SILVER_CATALOG s'il est différent.
"""
import argparse
import re
import sys
from pathlib import Path

from .common import get_spark
from .config import Settings


def statements(sql: str, catalog: str, namespace: str) -> list[str]:
    sql = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
    sql = sql.replace("lake.silver", f"{catalog}.{namespace}")
    return [s.strip() for s in re.split(r";\s*\n", sql) if s.strip()]


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--file", default=str(Path(__file__).resolve().parents[2] / "sql" / "silver_ddl.sql"))
    a = p.parse_args(argv)
    s = Settings()
    spark = get_spark("silver-ddl")
    for stmt in statements(Path(a.file).read_text(encoding="utf-8"), s.catalog, s.namespace):
        spark.sql(stmt)
        print(stmt.splitlines()[0])
    return 0


if __name__ == "__main__":
    sys.exit(main())
