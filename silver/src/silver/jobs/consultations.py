"""consultations — source 1.

Règles (Livrable 1, source 1 + décisions de modélisation) :
- rejet bloquant : identifiant de consultation absent ou non numérique, date
  absente / invalide / future, patient absent ou inexistant dans silver.patients ;
- valeurs remplaçables (-> NULL, rattachées à "Inconnu" par le Gold) :
  diagnostic absent ou inconnu du référentiel, professionnel absent ou inconnu ;
- établissement (id_organisation) déduit du lieu d'exercice du professionnel
  (activite_professionnel_sante) ; NULL si introuvable ou absent du référentiel
  établissements (le Gold le rattache à "Non rattaché", sk = -1) ;
- âge calculé avec la date de naissance complète, qui n'est pas conservée ;
- durée = heure de fin - heure de début ; fin avant début = passage de minuit
  (20:00 -> 02:00 = 360 min) ; durée nulle -> NULL ;
- nb_prescriptions = nombre de prescriptions liées (0 si aucune) ;
- doublons : conflit sur l'identifiant -> les deux en quarantaine ;
  double saisie (même patient, date, motif, professionnel, identifiants
  différents) -> la plus petite référence est gardée.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from ..common import (Quarantine, age_at, canon_id, clean, clean_len, date_formats, dedupe,
                      duree_minutes, norm_cim10, parse_date, parse_time_minutes, pseudo_join, to_long)
from ..context import Ctx, JobOutput
from . import _lookups as L

TABLE = "consultations"
MODE = "merge"


def build(ctx: Ctx, q: Quarantine) -> JobOutput:
    raw = ctx.bronze("s1_consultation")
    debut, fin = parse_time_minutes("heure_debut"), parse_time_minutes("heure_fin")
    df = raw.select(
        to_long("id_consultation").alias("id_consultation"),
        clean("id_consultation").alias("_cle"),
        parse_date("date", date_formats("s1_consultation")).alias("date_consultation"),
        canon_id("id_patient").alias("_pid"),
        canon_id("id_professionnel").alias("_prof"),
        norm_cim10("code_diag").alias("code_diag"),
        clean_len("motif", 50).alias("motif"),
        duree_minutes(debut, fin).alias("duree_minutes"),
        "_fichier", "_ligne", "_ingest_date",
    )
    df = q.split(df, F.col("id_consultation").isNull(), "identifiant de consultation absent ou non numérique")
    df = q.split(df, F.col("date_consultation").isNull(), "date de consultation absente ou invalide")
    df = q.split(df, F.col("date_consultation") > F.current_date(), "date de consultation dans le futur")
    df = q.split(df, F.col("_pid").isNull(), "patient absent")

    # Patient : pseudonyme, existence, âge à la date de consultation
    key = ctx.key
    df = pseudo_join(df, "_pid", "id_patient_pseudo", key)
    df = df.join(L.patients_connus(ctx), F.col("id_patient_pseudo") == F.col("_pp"), "left")
    df = q.split(df, F.col("_pconnu").isNull(), "patient inexistant")
    df = df.join(L.patients_naissance(ctx), "_pid", "left") \
           .withColumn("age_consultation", age_at(F.col("_naissance"), F.col("date_consultation")))

    # Professionnel : pseudonyme s'il existe dans silver.professionnels, sinon NULL
    profs = ctx.silver("professionnels").select(F.col("id_professionnel").alias("_ip"),
                                                F.lit(True).alias("_profconnu"))
    df = pseudo_join(df, "_prof", "_prof_pseudo", key) \
           .join(profs, F.col("_prof_pseudo") == F.col("_ip"), "left") \
           .withColumn("id_professionnel", F.when(F.col("_profconnu"), F.col("_prof_pseudo")))

    # Établissement : professionnel -> organisation (NULL si introuvable)
    orgs = L.organisation_du_professionnel(ctx)
    if orgs is not None:
        df = L.rattacher_organisation(ctx, df.join(orgs, "_prof", "left"))
    else:
        df = df.withColumn("id_organisation", F.lit(None).cast("string"))

    df = L.valider_diag(ctx, df)

    # Nombre de prescriptions
    presc = ctx.bronze("s1_prescription", all_dates=True)
    if presc is not None:
        nb = (presc.select(to_long("id_consultation").alias("id_consultation"))
              .groupBy("id_consultation").agg(F.count(F.lit(1)).cast("int").alias("nb_prescriptions")))
        df = df.join(nb, "id_consultation", "left")
    else:
        df = df.withColumn("nb_prescriptions", F.lit(None).cast("int"))
    df = df.withColumn("nb_prescriptions", F.coalesce("nb_prescriptions", F.lit(0)))

    values = ["date_consultation", "id_patient_pseudo", "id_professionnel", "code_diag",
              "id_organisation", "motif", "age_consultation", "duree_minutes", "nb_prescriptions"]
    df = dedupe(df, ["id_consultation"], values, q, policy="reject")

    # Doubles saisies : même patient, date, motif et professionnel, identifiants différents
    w = Window.partitionBy("id_patient_pseudo", "date_consultation", "motif", "_prof").orderBy("id_consultation")
    df = df.withColumn("_rk", F.row_number().over(w))
    double = (F.col("_rk") > 1) & F.col("motif").isNotNull() & F.col("_prof").isNotNull()
    df = q.split(df, double, "double saisie (même patient, date, motif, professionnel)", "doublon")

    return JobOutput(df.select(*values, "id_consultation", "_fichier", "_ingest_date"))
