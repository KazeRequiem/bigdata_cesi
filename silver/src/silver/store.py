"""Écriture des tables Silver.

- mode "iceberg" (production) : tables Apache Iceberg <catalog>.<namespace>.<table>,
  créées au préalable par sql/silver_ddl.sql. MERGE INTO pour l'incrémental,
  overwrite pour les référentiels remplacés en entier.
- mode "parquet" (tests locaux, sans Iceberg) : même sémantique émulée sur des
  dossiers Parquet.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings


class SilverStore:
    def __init__(self, spark: SparkSession, settings: Settings):
        self.spark, self.s = spark, settings
        self.iceberg = settings.storage == "iceberg"

    # ------------------------------------------------------------------ noms
    def table_id(self, name: str) -> str:
        return f"{self.s.catalog}.{self.s.namespace}.{name}"

    def _path(self, name: str) -> str:
        return f"{self.s.local_root.rstrip('/')}/{name}"

    def _fs(self, path: str):
        jvm = self.spark._jvm
        p = jvm.org.apache.hadoop.fs.Path(path)
        return p, p.getFileSystem(self.spark._jsc.hadoopConfiguration())

    # --------------------------------------------------------------- lecture
    def exists(self, name: str) -> bool:
        if self.iceberg:
            return self.spark.catalog.tableExists(self.table_id(name))
        p, fs = self._fs(self._path(name))
        return fs.exists(p)

    def read(self, name: str) -> DataFrame | None:
        if not self.exists(name):
            return None
        if self.iceberg:
            return self.spark.table(self.table_id(name))
        return self.spark.read.parquet(self._path(name))

    # -------------------------------------------------------------- écriture
    def _align(self, name: str, df: DataFrame) -> DataFrame:
        if self.iceberg:
            return df.select(*self.spark.table(self.table_id(name)).columns)
        return df

    def overwrite(self, name: str, df: DataFrame) -> None:
        """Remplacement complet (référentiels)."""
        if self.iceberg:
            self._align(name, df).writeTo(self.table_id(name)).overwrite(F.lit(True))
        else:
            self._replace_local(name, df)

    def append(self, name: str, df: DataFrame) -> None:
        if self.iceberg:
            self._align(name, df).writeTo(self.table_id(name)).append()
        else:
            df.write.mode("append").parquet(self._path(name))

    def merge(self, name: str, df: DataFrame, keys: list[str]) -> None:
        """Upsert idempotent sur la clé métier : relancer un job ne crée jamais
        de doublon. Une ligne existante n'est réécrite que si ses valeurs métier
        ont changé (les colonnes techniques "_..." sont ignorées pour ce test)."""
        if self.iceberg:
            df = self._align(name, df)
            # Premier chargement : simple append. Le MERGE sur une table vide écrit tout
            # dans une seule tâche avec un fichier ouvert par partition (décès : un par
            # mois depuis 1970) et sature la mémoire.
            if self.spark.table(self.table_id(name)).limit(1).count() == 0:
                df.writeTo(self.table_id(name)).append()
                return
            # localCheckpoint : MERGE refuse une source dont le plan contient du non
            # déterministe (monotonically_increasing_id de la lecture Bronze)
            df = df.localCheckpoint()
            view = f"_src_{name}"
            df.createOrReplaceTempView(view)
            on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
            business = [c for c in df.columns if not c.startswith("_") and c not in keys]
            changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in business) or "false"
            self.spark.sql(f"""
                MERGE INTO {self.table_id(name)} t
                USING {view} s
                ON {on}
                WHEN MATCHED AND ({changed}) THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            return
        old = self.read(name)
        if old is None:
            df.write.mode("overwrite").parquet(self._path(name))
            return
        kept = old.join(df.select(*keys), on=keys, how="left_anti")
        self._replace_local(name, kept.unionByName(df, allowMissingColumns=True))

    def _replace_local(self, name: str, df: DataFrame) -> None:
        path, tmp = self._path(name), self._path(name) + "__tmp"
        df.write.mode("overwrite").parquet(tmp)
        p, fs = self._fs(path)
        t, _ = self._fs(tmp)
        if fs.exists(p):
            fs.delete(p, True)
        fs.rename(t, p)
