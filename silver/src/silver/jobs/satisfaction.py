"""satisfaction — source 3 (e-Satis, HAS, via FTP).

Règles (Livrable 1, source 3) :
- année et type d'enquête lus dans le nom du fichier ;
- séparateur décimal unifié (virgule -> point) ;
- établissement identifié uniquement par son FINESS : absent, invalide ou
  introuvable dans le référentiel -> rejet ;
- score hors [0, 100] -> rejet ; classement hors {A, B, C, D, DI, NR, NV} -> rejet ;
- score global vide avec un classement A à D -> rejet ; vide avec DI/NR/NV -> conservé ;
- clé FINESS + année + type d'enquête ; conflit -> les deux en quarantaine.
"""
from pyspark.sql import functions as F

from ..common import Quarantine, clean, clean_len, dedupe, norm_finess, to_double, to_int
from ..context import Ctx, JobOutput

TABLE = "satisfaction"
MODE = "overwrite"  # contrat : remplacement complet ; toutes les ingestions sont relues

SCORES = ["score_global", "score_accueil", "score_pec_infirmier", "score_pec_medical",
          "score_chambre", "score_repas", "score_sortie", "taux_recommandation"]
CLASSES = ["A", "B", "C", "D", "DI", "NR", "NV"]


def type_enquete(fname):
    """Nom de fichier -> type d'enquête, en expressions Spark (pas d'UDF) :
    'resultats-esatis48h-mco-2020.csv' -> 'ESATIS48H_MCO',
    '...-esatisca-...' ou '...-ambulatoire-...' -> 'ESATISCA_MCO'."""
    n = F.lower(fname)
    tok = lambda t: n.rlike(rf"(^|[^a-z0-9]){t}([^a-z0-9]|$)")  # noqa: E731
    secteur = F.when(tok("ssr"), F.lit("SSR")).otherwise(F.lit("MCO"))
    return (F.when(n.contains("48h"), F.concat(F.lit("ESATIS48H_"), secteur))
             .when(tok("ca") | n.contains("esatisca") | n.contains("ambu"), F.concat(F.lit("ESATISCA_"), secteur))
             .when(tok("ssr"), F.lit("ESATIS_SSR")))


def build(ctx: Ctx, q: Quarantine) -> JobOutput:
    raw = ctx.bronze("s3_satisfaction", all_dates=True)
    fname = F.regexp_extract("_fichier", r"([^/]+)$", 1)
    annee = F.regexp_extract(fname, r"(20\d{2})", 1)
    df = raw.select(
        clean("finess_site").alias("_finess_brut"),
        norm_finess("finess_site").alias("finess_site"),
        F.when(annee != "", annee.cast("int")).alias("annee"),
        type_enquete(fname).alias("type_enquete"),
        clean_len("region_source", 60).alias("region_source"),
        to_int("nb_reponses").alias("nb_reponses"),
        *[to_double(s).alias(s) for s in SCORES],
        F.upper(clean("classement")).alias("classement"),
        clean_len("evolution", 30).alias("evolution"),
        "_fichier", "_ligne", "_ingest_date",
    )
    df = df.withColumn("_cle", F.concat_ws("|", "_finess_brut", "annee", "type_enquete"))

    df = q.split(df, F.col("_finess_brut").isNull(), "FINESS absent")
    df = q.split(df, F.col("finess_site").isNull(), "FINESS invalide")
    df = q.split(df, F.col("annee").isNull(), "année introuvable dans le nom du fichier")
    df = q.split(df, F.col("type_enquete").isNull(), "type d'enquête introuvable dans le nom du fichier")
    hors = None
    for s in SCORES:
        c = (F.col(s) < 0) | (F.col(s) > 100)
        hors = c if hors is None else (hors | c)
    df = q.split(df, hors, "score hors de la plage 0-100")
    df = q.split(df, F.col("classement").isNotNull() & ~F.col("classement").isin(CLASSES), "classement inconnu")
    df = q.split(df, F.col("score_global").isNull() & F.col("classement").isin("A", "B", "C", "D"),
                 "score global vide avec un classement A à D")

    etabs = (ctx.silver("etablissements").select(F.col("finess_site").alias("_f")).dropna().distinct()
             .withColumn("_ok", F.lit(True)))
    df = df.join(etabs, F.col("finess_site") == F.col("_f"), "left")
    # Établissement absent du référentiel : la ligne est valide mais hors du
    # périmètre du groupe -> quarantaine "hors_perimetre" (ne compte pas dans
    # le seuil de 5 %, sinon un fichier national ferait échouer le job).
    df = q.split(df, F.col("_ok").isNull(), "FINESS introuvable dans le référentiel établissements",
                 "hors_perimetre")

    for s in SCORES:
        df = df.withColumn(s, F.round(F.col(s), 2).cast("decimal(5,2)"))

    values = ["region_source", "nb_reponses", *SCORES, "classement", "evolution"]
    df = dedupe(df, ["finess_site", "annee", "type_enquete"], values, q, policy="reject")
    return JobOutput(df.select("finess_site", "annee", "type_enquete", *values, "_fichier", "_ingest_date"))
