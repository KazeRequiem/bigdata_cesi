"""diagnostics — source 1 (référentiel CIM-10).

Règles :
- code absent ou ne respectant pas le format CIM-10 -> rejet ;
- code normalisé sans point (J45.9 -> J459), de la même façon que dans les
  consultations et hospitalisations, sinon les jointures échouent ;
- chapitre (I à XXII) déduit des 3 premiers caractères (table OMS) ;
- conflit sur un même code -> libellé le plus complet / le plus récent.
"""
from pyspark.sql import functions as F

from ..common import Quarantine, cim10_chapitre, clean, clean_len, dedupe, norm_cim10
from ..context import Ctx, JobOutput

TABLE = "diagnostics"
MODE = "merge"


def build(ctx: Ctx, q: Quarantine) -> JobOutput:
    raw = ctx.bronze("s1_diagnostic")
    df = raw.select(
        norm_cim10("code_diag").alias("code_diag"),
        clean("code_diag").alias("_cle"),
        clean_len("libelle", 255).alias("libelle_diagnostic"),
        "_fichier", "_ligne", "_ingest_date",
    )
    df = q.split(df, F.col("_cle").isNull(), "code diagnostic absent")
    df = q.split(df, F.col("code_diag").isNull(), "code CIM-10 invalide")
    num, lib = cim10_chapitre(F.col("code_diag"))
    df = df.withColumn("code_chapitre", num).withColumn("libelle_chapitre", lib)
    df = dedupe(df, ["code_diag"], ["libelle_diagnostic"], q, latest_first=True)
    return JobOutput(df)
