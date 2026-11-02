"""hospitalisations — fichier Hospitalisations.csv (absent de la base PostgreSQL).

Règles (Livrable 1, source 1) :
- rejet bloquant : identifiant absent ou non numérique, date d'entrée absente
  ou future, patient absent ou inexistant, date de sortie antérieure à
  l'entrée, durée de séjour impossible à déterminer ou négative ;
- établissement : id_organisation du fichier ; NULL s'il est absent ou inconnu
  du référentiel (contrat : rattaché à "Non rattaché" en Gold) ;
- durée : date de sortie - date d'entrée si la sortie est connue, sinon durée
  fournie par la source, et la date de sortie est alors recalculée ;
- diagnostic inconnu du référentiel -> NULL ;
- âge à l'entrée calculé puis date de naissance abandonnée ;
- conflit sur un même identifiant -> les deux versions en quarantaine.
"""
from pyspark.sql import functions as F

from ..common import (Quarantine, age_at, canon_id, clean, date_formats, dedupe, norm_cim10,
                      parse_date, pseudo_join, to_int, to_long)
from ..context import Ctx, JobOutput
from . import _lookups as L

TABLE = "hospitalisations"
MODE = "merge"


def build(ctx: Ctx, q: Quarantine) -> JobOutput:
    raw = ctx.bronze("s1_hospitalisation")
    df = raw.select(
        to_long("id_hospitalisation").alias("id_hospitalisation"),
        clean("id_hospitalisation").alias("_cle"),
        parse_date("date_entree", date_formats("s1_hospitalisation")).alias("date_entree"),
        parse_date("date_sortie", date_formats("s1_hospitalisation")).alias("_sortie_src"),
        to_int("duree_jours").alias("_duree_src"),
        canon_id("id_patient").alias("_pid"),
        clean("id_organisation").alias("_org"),
        norm_cim10("code_diag").alias("code_diag"),
        "_fichier", "_ligne", "_ingest_date",
    )
    df = q.split(df, F.col("id_hospitalisation").isNull(), "identifiant d'hospitalisation absent ou non numérique")
    df = q.split(df, F.col("date_entree").isNull(), "date d'entrée absente ou invalide")
    df = q.split(df, F.col("date_entree") > F.current_date(), "date d'entrée dans le futur")
    df = q.split(df, F.col("_pid").isNull(), "patient absent")

    # Patient
    df = pseudo_join(df, "_pid", "id_patient_pseudo", ctx.key)
    df = df.join(L.patients_connus(ctx), F.col("id_patient_pseudo") == F.col("_pp"), "left")
    df = q.split(df, F.col("_pconnu").isNull(), "patient inexistant")
    df = df.join(L.patients_naissance(ctx), "_pid", "left") \
           .withColumn("age_hospitalisation", age_at(F.col("_naissance"), F.col("date_entree")))

    # Établissement : NULL si absent ou inconnu (non bloquant, contrat)
    df = L.rattacher_organisation(ctx, df)

    # Dates et durée
    df = q.split(df, F.col("_sortie_src") < F.col("date_entree"), "date de sortie antérieure à la date d'entrée")
    df = df.withColumn("duree_sejour_jours", F.coalesce(F.datediff("_sortie_src", "date_entree"), F.col("_duree_src")))
    df = q.split(df, F.col("duree_sejour_jours").isNull(), "durée de séjour impossible à déterminer")
    df = q.split(df, F.col("duree_sejour_jours") < 0, "durée de séjour négative")
    df = df.withColumn("date_sortie", F.coalesce("_sortie_src", F.date_add("date_entree", F.col("duree_sejour_jours"))))

    df = L.valider_diag(ctx, df)

    values = ["date_entree", "date_sortie", "id_patient_pseudo", "code_diag", "id_organisation",
              "age_hospitalisation", "duree_sejour_jours"]
    df = dedupe(df, ["id_hospitalisation"], values, q, policy="reject")
    return JobOutput(df.select("id_hospitalisation", *values, "_fichier", "_ingest_date"))
