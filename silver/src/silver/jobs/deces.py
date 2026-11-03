"""deces — source 4 (fichier des décès INSEE, via FTP).

Règles (Livrable 1, source 4 + contrat) :
- noms, prénoms et lieu de naissance supprimés (jamais lus au-delà de Bronze) ;
- date de décès absente, invalide ou future -> rejet ;
- date partielle (mois ou jour à 00) : mois/jour remplacés par 01 (00/00 -> 1er
  janvier). Pas de colonne date_partielle : décision du Gold, les indicateurs
  de mortalité sont annuels ;
- naissance postérieure au décès -> rejet ; âge hors [0, 120] -> NULL ;
- sexe 1 -> M, 2 -> F ;
- lieu : ancien code recodé vers la commune actuelle (passage_communes) ;
  code 99xxx (décès à l'étranger) -> quarantaine "hors_perimetre" ;
  code inconnu du COG -> NULL (géographie "Inconnue" en Gold) ;
- doublons : clé lieu + date + numéro d'acte (les fichiers INSEE se recouvrent).
  Cette clé est stockée pseudonymisée dans la colonne technique _id_deces
  (sert au MERGE, jamais exportée en staging) ; les décès ne sont jamais reliés
  aux patients (Livrable 1).
"""
from pyspark.sql import functions as F

from ..common import Quarantine, clean, dedupe, norm_insee, pseudo
from ..context import Ctx, JobOutput

TABLE = "deces"
MODE = "merge"


def _partial_date(c):
    """Date INSEE avec éventuels 00 -> (date, partielle). Formats acceptés :
    AAAAMMJJ (fichier INSEE), AAAA-MM-JJ, JJ/MM/AAAA (export CSV)."""
    c = clean(c)
    c = (F.when(c.rlike(r"^\d{2}/\d{2}/\d{4}"), F.concat(c.substr(7, 4), c.substr(4, 2), c.substr(1, 2)))
          .otherwise(F.regexp_replace(F.substring(c, 1, 10), r"[^0-9]", "")))
    ok = c.rlike(r"^\d{8}$")
    y, m, d = c.substr(1, 4).cast("int"), c.substr(5, 2).cast("int"), c.substr(7, 2).cast("int")
    m2 = F.when(m == 0, 1).otherwise(m)
    d2 = F.when((m == 0) | (d == 0), 1).otherwise(d)
    date = F.when(ok & (y > 0), F.make_date(y, m2, d2))
    return date, ok & ((m == 0) | (d == 0))


def build(ctx: Ctx, q: Quarantine) -> JobOutput:
    raw = ctx.bronze("s4_deces")
    deces, _ = _partial_date("date_deces")
    naiss, _ = _partial_date("date_naissance")
    lieu = norm_insee("lieu_deces")
    df = raw.select(
        deces.alias("date_deces"), naiss.alias("_naissance"),
        F.when(clean("sexe") == "1", "M").when(clean("sexe") == "2", "F").alias("sexe"),
        lieu.alias("code_commune_deces"),
        pseudo(F.concat_ws("|", clean("lieu_deces"), clean("date_deces"), clean("numero_acte")),
               ctx.key).alias("_id_deces"),
        "_fichier", "_ligne", "_ingest_date",
    ).withColumn("_cle", F.col("_id_deces"))

    df = q.split(df, F.col("date_deces").isNull(), "date de décès absente ou invalide")
    df = q.split(df, F.col("date_deces") > F.current_date(), "date de décès dans le futur")
    df = q.split(df, F.col("_naissance") > F.col("date_deces"), "date de décès antérieure à la naissance")
    df = q.split(df, F.col("code_commune_deces").startswith("99"), "décès à l'étranger", "hors_perimetre")

    age = F.floor(F.months_between("date_deces", "_naissance") / 12).cast("int")
    df = df.withColumn("age_deces", F.when((age >= 0) & (age <= 120), age))

    passage = ctx.silver("passage_communes")
    if passage is not None:
        df = (df.join(passage, df.code_commune_deces == passage.code_ancien, "left")
              .withColumn("code_commune_deces", F.coalesce("code_actuel", "code_commune_deces"))
              .drop("code_ancien", "code_actuel"))
    communes = ctx.silver("communes")
    if communes is not None:
        known = communes.select(F.col("code_commune_insee").alias("_c"), F.lit(True).alias("_ok"))
        df = (df.join(known, df.code_commune_deces == known._c, "left")
              .withColumn("code_commune_deces", F.when(F.col("_ok"), F.col("code_commune_deces")))
              .drop("_c", "_ok"))

    values = ["date_deces", "code_commune_deces", "sexe", "age_deces"]
    df = dedupe(df, ["_id_deces"], values, q)
    return JobOutput(df.select(*values, "_id_deces", "_fichier", "_ingest_date"))
