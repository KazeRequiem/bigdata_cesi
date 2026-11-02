"""Contexte passé à chaque job : accès Bronze, Silver, clé, date d'ingestion."""
from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession

from .common import hmac_key, read_bronze
from .config import Settings
from .store import SilverStore


@dataclass
class JobOutput:
    """Résultat d'un job : la table principale + tables techniques annexes."""
    df: DataFrame
    extras: dict[str, DataFrame] = field(default_factory=dict)


@dataclass
class Ctx:
    spark: SparkSession
    settings: Settings
    store: SilverStore
    run_id: str
    ingest_date: str | None = None  # None = toutes les ingestions présentes en Bronze

    def bronze(self, source: str, all_dates: bool = False) -> DataFrame | None:
        """all_dates=True : ignore ingest_date (référentiels nécessaires en entier,
        ex. date de naissance des patients pour calculer l'âge)."""
        return read_bronze(self.spark, self.settings, source, None if all_dates else self.ingest_date)

    def silver(self, name: str) -> DataFrame | None:
        return self.store.read(name)

    @property
    def key(self) -> bytes:
        return hmac_key(self.settings)
