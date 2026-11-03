"""communes — référentiel géographique (fichier COG de l'INSEE).

Règles :
- on garde les communes (COM) et les arrondissements municipaux de Paris,
  Lyon, Marseille (ARM), indispensables pour le fichier des décès ;
- département et région d'un arrondissement repris de sa commune parente ;
- table annexe passage_communes : ancien code -> code actuel (communes
  déléguées/associées et, si le fichier est fourni, mouvements de communes).
  Elle sert à recoder les décès survenus dans une commune fusionnée depuis.
Remplacement complet à chaque chargement (référentiel annuel).
"""
from pyspark.sql import functions as F

from ..common import Quarantine, clean, clean_len, dedupe, latest_by, norm_insee
from ..context import Ctx, JobOutput

TABLE = "communes"
MODE = "overwrite"


def _last_snapshot(df):
    mx = df.agg(F.max("_ingest_date")).first()[0]
    return df if mx is None else df.filter(F.col("_ingest_date") == F.lit(mx))


def build(ctx: Ctx, q: Quarantine) -> JobOutput:
    com = _last_snapshot(ctx.bronze("cog_communes", all_dates=True))
    dep = _last_snapshot(ctx.bronze("cog_departements", all_dates=True))
    reg = _last_snapshot(ctx.bronze("cog_regions", all_dates=True))

    com = com.select(
        F.upper(clean("typecom")).alias("typecom"),
        norm_insee("com").alias("code_commune_insee"),
        clean("com").alias("_cle"),
        F.upper(clean("dep")).alias("code_departement"),
        F.lpad(clean("reg"), 2, "0").alias("code_region"),
        clean_len("libelle", 100).alias("libelle_commune"),
        norm_insee("comparent").alias("comparent"),
        "_fichier", "_ligne", "_ingest_date",
    )

    actives = com.filter(F.col("typecom").isin("COM", "ARM"))
    actives = q.split(actives, F.col("code_commune_insee").isNull(), "code commune invalide")

    # Arrondissements : département / région repris de la commune parente
    parents = actives.filter("typecom = 'COM'").select(
        F.col("code_commune_insee").alias("comparent"),
        F.col("code_departement").alias("p_dep"), F.col("code_region").alias("p_reg"))
    actives = (actives.join(parents, "comparent", "left")
               .withColumn("code_departement", F.coalesce("code_departement", "p_dep"))
               .withColumn("code_region", F.coalesce("code_region", "p_reg"))
               .drop("p_dep", "p_reg"))

    dep = dep.select(F.upper(clean("dep")).alias("code_departement"),
                     clean_len("libelle", 60).alias("libelle_departement")).dropDuplicates(["code_departement"])
    reg = reg.select(F.lpad(clean("reg"), 2, "0").alias("code_region"),
                     clean_len("libelle", 60).alias("libelle_region")).dropDuplicates(["code_region"])

    out = (actives.join(dep, "code_departement", "left").join(reg, "code_region", "left"))
    out = dedupe(out, ["code_commune_insee"],
                 ["libelle_commune", "code_departement", "code_region"], q)

    # ---- passage ancien code -> code actuel --------------------------------
    current = out.select("code_commune_insee")
    delegues = (com.filter(F.col("typecom").isin("COMD", "COMA"))
                .select(F.col("code_commune_insee").alias("code_ancien"),
                        F.col("comparent").alias("code_actuel")))
    mvt = ctx.bronze("cog_mouvements", all_dates=True)
    if mvt is not None:
        mvt = latest_by(
            mvt.select(norm_insee("com_av").alias("code_ancien"), norm_insee("com_ap").alias("code_actuel"),
                       clean("date_eff").alias("date_eff"), F.upper(clean("typecom_ap")).alias("typecom_ap"),
                       "_ingest_date", "_ligne")
               .filter("typecom_ap = 'COM' AND code_ancien <> code_actuel")
               .withColumn("_ingest_date", F.to_date("date_eff")), ["code_ancien"])
        delegues = delegues.unionByName(mvt.select("code_ancien", "code_actuel"))
    passage = (delegues.dropna()
               .join(current.withColumnRenamed("code_commune_insee", "code_ancien"), "code_ancien", "left_anti")
               .dropDuplicates(["code_ancien"]))
    # Fusions en chaîne (A -> B -> C) : on suit la chaîne jusqu'à un code actuel
    for _ in range(5):
        nxt = passage.select(F.col("code_ancien").alias("code_actuel"), F.col("code_actuel").alias("suivant"))
        passage = (passage.join(nxt, "code_actuel", "left")
                   .select("code_ancien", F.coalesce("suivant", "code_actuel").alias("code_actuel")))
    passage = passage.join(current.withColumnRenamed("code_commune_insee", "code_actuel"), "code_actuel", "inner")

    return JobOutput(out, {"passage_communes": passage.select("code_ancien", "code_actuel")})
