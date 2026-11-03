"""etablissements — source 2 (CSV établissements).

Règles (Livrable 1, source 2) :
- id_organisation absent -> rejet ; FINESS site renseigné mais mal formé -> rejet ;
- FINESS site : la colonne finess_site du fichier réel est vide à 99,95 %. Pour
  les organisations de type établissement, l'identifiant vaut "F" + FINESS
  géographique (ex. F010000024 = CH de Fleyriat, FINESS 010000024, vérifié
  contre les fichiers e-Satis : 99 % des finess_geo e-Satis y sont retrouvés).
  Variante "F" + FINESS + date JJMMAAAA (ex. F30001086501012007 -> 300010865),
  37 % des organisations F.
  finess_site = colonne finess_site si renseignée, sinon caractères 2 à 10 de
  l'identifiant.
  Les autres organisations (R... = cabinets RPPS, C... = SIRET) n'ont pas de FINESS ;
- FINESS juridique, SIRET, code postal mal formés -> NULL ;
- code commune absent du COG -> NULL (rattachement géographique impossible) ;
- e-mail, téléphone et autres colonnes hors modèle : non repris ;
- une version par (id_organisation, date_extraction) : la table est remplacée à
  chaque chargement par TOUTES les versions présentes en Bronze, l'historique est
  donc conservé pour le SCD2 du Gold ; en cas de conflit, la ligne la plus
  complète est gardée.
"""
from pyspark.sql import functions as F

from ..common import (Quarantine, clean, clean_len, dedupe, norm_code_postal, norm_finess,
                      norm_insee)
from ..context import Ctx, JobOutput

TABLE = "etablissements"
MODE = "overwrite"  # contrat : remplacement complet ; toutes les ingestions sont relues


def build(ctx: Ctx, q: Quarantine) -> JobOutput:
    raw = ctx.bronze("s2_etablissements", all_dates=True)

    # Date du fichier : date dans le nom (AAAAMMJJ ou AAAA-MM-JJ), sinon date d'ingestion
    fname = F.regexp_extract("_fichier", r"([^/]+)$", 1)
    d8 = F.regexp_extract(fname, r"(20\d{6})", 1)
    d10 = F.regexp_extract(fname, r"(20\d{2}-\d{2}-\d{2})", 1)
    date_extraction = F.coalesce(F.to_date(F.when(d10 != "", d10)),
                                 F.to_date(F.when(d8 != "", d8), "yyyyMMdd"),
                                 F.col("_ingest_date"))

    df = raw.select(
        clean("id_organisation").alias("id_organisation"),
        clean("id_organisation").alias("_cle"),
        clean("finess_site").alias("_finess_brut"),
        F.coalesce(norm_finess("finess_site"),
                   F.when(clean("id_organisation").rlike(r"^F(\d{9}|2[AB]\d{7})(\d{8})?$"),
                          F.substring(clean("id_organisation"), 2, 9))).alias("finess_site"),
        norm_finess("finess_juridique").alias("finess_juridique"),
        clean_len("raison_sociale", 255).alias("raison_sociale"),
        F.when(F.regexp_replace(clean("siret"), r"\s", "").rlike(r"^\d{14}$"),
               F.regexp_replace(clean("siret"), r"\s", "")).alias("siret"),
        clean_len("adresse_complete", 255).alias("adresse_complete"),
        norm_code_postal("code_postal").alias("code_postal"),
        norm_insee("code_commune_insee").alias("code_commune_insee"),
        date_extraction.alias("date_extraction"),
        "_fichier", "_ligne", "_ingest_date",
    )

    df = q.split(df, F.col("id_organisation").isNull(), "id_organisation absent")
    df = q.split(df, F.length("id_organisation") > 20, "id_organisation trop long (> 20)")
    df = q.split(df, F.col("_finess_brut").isNotNull() & F.col("finess_site").isNull(),
                 "FINESS site invalide (9 caractères attendus)")
    df = q.split(df, F.col("date_extraction").isNull(), "date d'extraction introuvable")

    # Rattachement à la commune : code connu du COG, ou ancien code recodé
    communes = ctx.silver("communes")
    if communes is not None:
        passage = ctx.silver("passage_communes")
        if passage is not None:
            df = (df.join(passage.withColumnRenamed("code_ancien", "code_commune_insee"),
                          "code_commune_insee", "left")
                  .withColumn("code_commune_insee", F.coalesce("code_actuel", "code_commune_insee"))
                  .drop("code_actuel"))
        known = communes.select(F.col("code_commune_insee").alias("_c"), F.lit(True).alias("_ok"))
        df = (df.join(known, df.code_commune_insee == known._c, "left")
              .withColumn("code_commune_insee", F.when(F.col("_ok"), F.col("code_commune_insee")))
              .drop("_c", "_ok"))

    values = ["finess_site", "finess_juridique", "raison_sociale", "siret",
              "adresse_complete", "code_postal", "code_commune_insee"]
    df = dedupe(df, ["id_organisation", "date_extraction"], values, q)
    return JobOutput(df.drop("_finess_brut"))
