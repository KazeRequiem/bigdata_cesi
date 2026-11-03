"""Correspondances partagées par consultations et hospitalisations."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..common import canon_id, clean, date_formats, latest_by, parse_date
from ..context import Ctx


def patients_naissance(ctx: Ctx) -> DataFrame:
    """id brut -> date de naissance complète (Bronze, version la plus récente).
    Sert uniquement à calculer l'âge à l'événement ; la date n'est jamais écrite
    en Silver (Livrable 1 : calcul de l'âge puis suppression de la date)."""
    p = ctx.bronze("s1_patient", all_dates=True)
    p = p.select(canon_id("id_patient").alias("_pid"), parse_date("date_naissance", date_formats("s1_patient")).alias("_naissance"),
                 "_ingest_date", "_ligne").filter("_pid IS NOT NULL")
    return latest_by(p, ["_pid"]).select("_pid", "_naissance")


def patients_connus(ctx: Ctx) -> DataFrame:
    """Pseudonymes présents dans silver.patients (contrôle 'patient inexistant')."""
    return ctx.silver("patients").select(F.col("id_patient_pseudo").alias("_pp")).withColumn("_pconnu", F.lit(True))



def organisations_connues(ctx: Ctx) -> DataFrame:
    """id_organisation présents dans silver.etablissements (toutes versions)."""
    return (ctx.silver("etablissements").select(F.col("id_organisation").alias("_oc"))
            .distinct().withColumn("_oconnue", F.lit(True)))


def rattacher_organisation(ctx: Ctx, df: DataFrame, col: str = "_org") -> DataFrame:
    """id_organisation = col si l'organisation existe dans le référentiel, sinon
    NULL (le Gold rattache les NULL à "Non rattaché", sk = -1)."""
    o = organisations_connues(ctx)
    return (df.join(o, df[col] == o["_oc"], "left")
              .withColumn("id_organisation", F.when(F.col("_oconnue"), F.col(col)))
              .drop("_oc", "_oconnue"))



def valider_diag(ctx: Ctx, df: DataFrame) -> DataFrame:
    """Code CIM-10 absent du référentiel -> NULL (valeur remplaçable, le Gold
    le rattachera au membre 'Inconnu')."""
    d = ctx.silver("diagnostics").select(F.col("code_diag").alias("_cd")).withColumn("_dconnu", F.lit(True))
    return (df.join(d, df.code_diag == d._cd, "left")
              .withColumn("code_diag", F.when(F.col("_dconnu"), F.col("code_diag")))
              .drop("_cd", "_dconnu"))


def organisation_du_professionnel(ctx: Ctx) -> DataFrame | None:
    """id professionnel brut -> id_organisation de son lieu d'exercice
    (fichier activite_professionnel_sante). Un professionnel peut avoir plusieurs
    lieux : on retient en priorité un établissement (identifiant "F..." = FINESS),
    puis le plus petit identifiant. Choix déterministe, limite documentée."""
    a = ctx.bronze("s2_activite", all_dates=True)
    if a is None:
        return None
    a = a.select(canon_id("identifiant").alias("_prof"), clean("id_organisation").alias("_org")).dropna()
    rang = F.struct((~F.col("_org").startswith("F")).alias("pas_etab"), F.col("_org").alias("org"))
    return a.groupBy("_prof").agg(F.min(rang).alias("_r")).select("_prof", F.col("_r.org").alias("_org"))
