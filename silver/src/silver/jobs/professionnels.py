"""professionnels — source 1.

Règles :
- identifiant absent -> rejet ;
- identifiant RPPS/ADELI remplacé par son pseudonyme HMAC-SHA256 (Livrable 1,
  p. 34 : identifiants patient ET professionnel pseudonymisés) ;
- nom et prénom non repris ;
- type d'identifiant normalisé : RPPS / ADELI (codes ANS 8 / 0 acceptés) ;
- libellé de spécialité et fonction repris de la table Specialites
  (jointure sur code_specialite) ;
- conflit sur un même identifiant -> ligne la plus récente puis la plus complète.
"""
from pyspark.sql import functions as F

from ..common import Quarantine, canon_id, clean, clean_len, dedupe, latest_by, pseudo
from ..context import Ctx, JobOutput

TABLE = "professionnels"
MODE = "merge"


def build(ctx: Ctx, q: Quarantine) -> JobOutput:
    raw = ctx.bronze("s1_professionnel")
    t = F.upper(clean("type_identifiant"))
    df = raw.select(
        canon_id("identifiant").alias("_id_brut"),
        F.when(t.isin("RPPS", "8"), "RPPS").when(t.isin("ADELI", "0"), "ADELI").alias("type_identifiant"),
        clean_len("civilite", 8).alias("civilite"),
        clean_len("categorie_professionnelle", 10).alias("categorie_professionnelle"),
        clean_len("profession", 50).alias("profession"),
        clean_len("code_specialite", 32).alias("code_specialite"),
        "_fichier", "_ligne", "_ingest_date",
    )
    spe = ctx.bronze("s1_specialites", all_dates=True)
    spe = latest_by(spe.select(clean("code_specialite").alias("code_specialite"),
                               clean_len("libelle_specialite", 60).alias("libelle_specialite"),
                               clean_len("fonction", 50).alias("fonction"), "_ingest_date", "_ligne")
                    .filter("code_specialite IS NOT NULL"), ["code_specialite"])
    df = df.join(spe.drop("_ingest_date", "_ligne"), "code_specialite", "left")
    df = df.withColumn("id_professionnel", pseudo(F.col("_id_brut"), ctx.key)) \
           .withColumn("_cle", F.col("id_professionnel"))
    df = q.split(df, F.col("_id_brut").isNull(), "identifiant professionnel absent")

    values = ["type_identifiant", "civilite", "categorie_professionnelle", "profession",
              "code_specialite", "libelle_specialite", "fonction"]
    df = dedupe(df, ["id_professionnel"], values, q, latest_first=True)
    return JobOutput(df.drop("_id_brut"))
