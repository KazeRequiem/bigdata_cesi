"""patients — source 1.

Règles (Livrable 1, pseudonymisation) :
- identifiant absent -> rejet ; identifiant remplacé par HMAC-SHA256 ;
- nom, prénom, adresse, e-mail, téléphone, n° de sécurité sociale : non repris ;
- date de naissance réduite à l'année (hors [1900, année courante] -> NULL) ;
- code postal réduit au département de résidence (ÉCART au contrat : la source
  ne donne pas de code commune INSEE, et le Livrable 1 ne garde que le département) ;
- conflit sur un même patient -> version la plus récente.
"""
from pyspark.sql import functions as F

from ..common import (Quarantine, canon_id, date_formats, dedupe, departement_from_cp,
                      norm_code_postal, norm_sexe, parse_date, pseudo)
from ..context import Ctx, JobOutput

TABLE = "patients"
MODE = "merge"


def build(ctx: Ctx, q: Quarantine) -> JobOutput:
    raw = ctx.bronze("s1_patient")
    naissance = parse_date("date_naissance", date_formats("s1_patient"))  # M/d/yyyy dans la base
    annee = F.year(naissance)
    df = raw.select(
        canon_id("id_patient").alias("_id_brut"),
        norm_sexe("sexe").alias("sexe"),
        F.when((annee >= 1900) & (annee <= F.year(F.current_date())), annee).alias("annee_naissance"),
        departement_from_cp(norm_code_postal("code_postal")).alias("code_departement"),
        "_fichier", "_ligne", "_ingest_date",
    )
    df = df.withColumn("id_patient_pseudo", pseudo(F.col("_id_brut"), ctx.key)) \
           .withColumn("_cle", F.col("id_patient_pseudo"))
    df = q.split(df, F.col("_id_brut").isNull(), "identifiant patient absent")

    # Département inconnu du COG -> NULL
    communes = ctx.silver("communes")
    if communes is not None:
        deps = communes.select(F.col("code_departement").alias("_d")).distinct().withColumn("_ok", F.lit(True))
        df = (df.join(deps, df.code_departement == deps._d, "left")
              .withColumn("code_departement", F.when(F.col("_ok"), F.col("code_departement")))
              .drop("_d", "_ok"))

    df = dedupe(df, ["id_patient_pseudo"], ["sexe", "annee_naissance", "code_departement"], q,
                latest_first=True)
    return JobOutput(df.drop("_id_brut"))
