"""Briques communes à tous les jobs Silver.

- session Spark
- lecture de la couche Bronze avec correspondance des colonnes (config.SOURCES)
- normalisation des valeurs (dates, codes INSEE/FINESS, sexe...)
- pseudonymisation HMAC-SHA256
- quarantaine (rejets / doublons / hors périmètre) et dédoublonnage
"""
from __future__ import annotations

import hashlib
import hmac
import os
import re
import unicodedata
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import INSEE_FIXE, SOURCES, Settings

# ===========================================================================
# Session
# ===========================================================================

def get_spark(app: str = "silver") -> SparkSession:
    """Session Spark. La configuration Iceberg / S3 (catalogue, endpoint
    SeaweedFS, identifiants) est fournie par spark-submit ou spark-defaults."""
    return (
        SparkSession.builder.appName(app)
        # Une valeur mal formée doit donner NULL (puis un rejet contrôlé),
        # pas faire planter tout le job.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.session.timeZone", "Europe/Paris")
        .getOrCreate()
    )


# ===========================================================================
# Lecture Bronze
# ===========================================================================

def _norm_name(name: str) -> str:
    """'Identifiant_Organisation ' -> 'identifiantorganisation' (BOM, accents, casse)."""
    name = unicodedata.normalize("NFKD", name.replace("﻿", ""))
    name = "".join(ch for ch in name if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]", "", name.lower())


def apply_mapping(df: DataFrame, mapping: dict[str, list[str]]) -> DataFrame:
    """Renomme les colonnes source vers les noms logiques ; colonne absente -> NULL.
    Toutes les valeurs passent en texte : le typage est fait par chaque job."""
    by_norm = {_norm_name(c): c for c in df.columns}
    cols = []
    for logical, candidates in mapping.items():
        src = next((by_norm[_norm_name(c)] for c in candidates if _norm_name(c) in by_norm), None)
        cols.append((F.col(f"`{src}`").cast("string") if src else F.lit(None).cast("string")).alias(logical))
    tech = [F.col(c) for c in df.columns if c.startswith("_")]
    return df.select(*cols, *tech)


def _hadoop_files(spark: SparkSession, root: str) -> list[str]:
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(root)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return []
    out, it = [], fs.listFiles(path, True)
    while it.hasNext():
        p = it.next().getPath()
        name = p.getName()
        if not name.startswith(("_", ".", "~$")):
            out.append(p.toString())
    return sorted(out)


# Date d'ingestion dans le chemin : bronze/<source>/2026-11-02/<fichier> (rangement
# NiFi du groupe) ; la forme ingest_date=2026-11-02 est aussi acceptée.
_INGEST_RE = r"(?:ingest_date=|/)(\d{4}-\d{2}-\d{2})(?:/|$)"
MANIFEST_SUFFIX = ".manifest.json"


def _with_lineage(df: DataFrame, file_col: Column) -> DataFrame:
    """Ajoute _fichier, _ligne (numéro de ligne dans le fichier) et _ingest_date."""
    df = df.withColumn("_fichier", file_col).withColumn("_mid", F.monotonically_increasing_id())
    w = Window.partitionBy("_fichier").orderBy("_mid")
    return (df.withColumn("_ligne", F.row_number().over(w))
              .withColumn("_ingest_date", F.to_date(F.regexp_extract("_fichier", _INGEST_RE, 1)))
              .drop("_mid"))


def _detect_sep(first_line: str) -> str:
    counts = {s: first_line.count(s) for s in (";", ",", "\t", "|")}
    return max(counts, key=counts.get)


def _read_one_csv(spark, f, encoding):
    raw = spark.read.option("sep", "\u0001").option("encoding", encoding).csv(f)
    first = raw.first()
    if first is None:
        return None
    sep = _detect_sep(first[0] or "")
    return (spark.read.option("header", True).option("sep", sep).option("encoding", encoding)
            .option("quote", '"').option("escape", '"').option("multiLine", True).csv(f))


def _read_xlsx(spark, f):
    """Lit la 1re feuille d'un .xlsx (petits fichiers e-Satis) : tout en texte.
    Le fichier est lu via Hadoop (fonctionne aussi sur s3a://), puis openpyxl."""
    import io

    from openpyxl import load_workbook  # dépendance du driver uniquement

    data = spark.sparkContext.binaryFiles(f).first()[1]
    ws = load_workbook(io.BytesIO(data), read_only=True, data_only=True).worksheets[0]
    rows = ws.iter_rows(values_only=True)
    header = [str(h).strip() if h is not None else f"col{i}" for i, h in enumerate(next(rows))]

    def txt(v):
        if v is None:
            return None
        if isinstance(v, float) and v.is_integer():
            return str(int(v))  # 10000024.0 -> "10000024" (le FINESS est re-complété ensuite)
        return str(v)

    body = [[txt(v) for v in r] for r in rows if any(v is not None for v in r)]
    schema = T.StructType([T.StructField(h, T.StringType()) for h in header])
    return spark.createDataFrame(body, schema)


def _read_insee_fixe(spark, f, encoding):
    raw = spark.read.option("sep", "\u0001").option("encoding", encoding).csv(f).toDF("l")
    cols = {name: F.substring("l", pos, length) for name, (pos, length) in INSEE_FIXE.items()}
    # Noms alignés sur la version CSV data.gouv pour partager la même correspondance
    return raw.select(
        cols["sexe"].alias("sexe"), cols["date_naissance"].alias("datenaiss"),
        cols["date_deces"].alias("datedeces"), cols["lieu_deces"].alias("lieudeces"),
        cols["numero_acte"].alias("actedeces"),
    )


def _sha256(spark, f: str) -> str:
    data = spark.sparkContext.binaryFiles(f).first()[1]
    return hashlib.sha256(data).hexdigest()


def _check_manifest(spark, f: str, manifest: str) -> None:
    """Contrôle d'entrée (Livrable 1) : l'empreinte SHA-256 du fichier doit être
    celle notée par NiFi dans son manifeste, sinon le fichier est refusé en entier."""
    import json
    txt = "\n".join(r[0] for r in spark.read.text(manifest, wholetext=True).collect())
    expected = (json.loads(txt).get("content_SHA-256") or "").lower()
    if expected and _sha256(spark, f) != expected:
        raise ValueError(f"Fichier refusé : empreinte SHA-256 différente du manifeste NiFi ({f})")


def _file_format(fmt: str, f: str) -> str:
    """format "auto" : déduit de l'extension (NiFi dépose les fichiers tels quels)."""
    if fmt != "auto":
        return fmt
    ext = f.lower().rsplit(".", 1)[-1]
    return {"parquet": "parquet", "avro": "avro", "csv": "csv", "txt": "csv", "tsv": "csv",
            "xlsx": "xlsx", "xlsm": "xlsx", "json": "json"}.get(ext, "csv")


def _avro_time_fields(spark, f: str) -> dict[str, int]:
    """Champs time-millis / time-micros du schéma Avro -> diviseur vers les secondes.
    spark-avro ne gère pas ces types logiques : il les lit en simple entier."""
    import json
    jvm = spark._jvm
    src = jvm.org.apache.avro.mapred.FsInput(jvm.org.apache.hadoop.fs.Path(f), spark._jsc.hadoopConfiguration())
    rd = jvm.org.apache.avro.file.DataFileReader.openReader(src, jvm.org.apache.avro.generic.GenericDatumReader())
    try:
        schema = json.loads(rd.getSchema().toString())
    finally:
        rd.close()
    out = {}
    for fld in schema.get("fields", []):
        types = fld["type"] if isinstance(fld["type"], list) else [fld["type"]]
        for t in types:
            if isinstance(t, dict) and t.get("logicalType") in ("time-millis", "time-micros"):
                out[fld["name"]] = 1000 if t["logicalType"] == "time-millis" else 1_000_000
    return out


def _read_avro(spark, files: list[str]) -> DataFrame:
    """Avro NiFi (ExecuteSQL, types logiques activés) : date et timestamp arrivent typés
    (relus par parse_date), les heures sont remises en texte HH:mm:ss."""
    df = spark.read.format("avro").load(files)
    for name, div in _avro_time_fields(spark, files[0]).items():
        s = F.floor(F.col(f"`{name}`") / div)
        hms = F.format_string("%02d:%02d:%02d", F.floor(s / 3600), F.floor(s / 60) % 60, s % 60)
        df = df.withColumn(name, F.when(s.isNotNull(), hms))
    return df


def read_bronze(spark: SparkSession, settings: Settings, source: str,
                ingest_date: str | None = None) -> DataFrame | None:
    """Lit une source Bronze et renvoie les colonnes logiques + _fichier, _ligne,
    _ingest_date. Renvoie None si la source est facultative et absente.
    - include : regex appliquée au chemin relatif à la racine de la source ;
    - manifestes NiFi (*.manifest.json) : jamais lus comme données, mais servent
      à vérifier l'empreinte de chaque fichier (settings.verify_manifest)."""
    cfg = SOURCES[source]
    root = f"{settings.bronze_root.rstrip('/')}/{cfg['path']}".rstrip("/")
    all_files = _hadoop_files(spark, root)
    manifests = {f for f in all_files if f.endswith(MANIFEST_SUFFIX)}
    files = [f for f in all_files if f not in manifests]
    if cfg.get("include"):
        rx = re.compile(cfg["include"])
        files = [f for f in files if rx.search(f[len(root):].lstrip("/"))]
    if ingest_date:
        files = [f for f in files if re.search(rf"(?:ingest_date=|/){ingest_date}(?:/|$)", f)]
    if not files:
        if cfg.get("optional"):
            return None
        raise FileNotFoundError(f"Aucun fichier Bronze pour {source} sous {root}"
                                + (f" (filtre {cfg['include']})" if cfg.get("include") else ""))

    if settings.verify_manifest:
        for f in files:
            if f + MANIFEST_SUFFIX in manifests:
                _check_manifest(spark, f, f + MANIFEST_SUFFIX)

    fmt, enc = cfg.get("format", "auto"), cfg.get("encoding", "UTF-8")
    # Formats avec schéma (Parquet, Avro) : lus en une fois.
    parts = []
    for kind in ("parquet", "avro"):
        batch = [f for f in files if _file_format(fmt, f) == kind]
        if batch:
            df = spark.read.parquet(*batch) if kind == "parquet" else _read_avro(spark, batch)
            parts.append(apply_mapping(_with_lineage(df, F.input_file_name()), cfg["columns"]))

    # Fichiers plats : lus un par un (séparateur et colonnes peuvent changer
    # d'une année à l'autre, ex. e-Satis), puis réunis après correspondance.
    for f in files:
        kind = _file_format(fmt, f)
        if kind in ("parquet", "avro"):
            continue
        if kind == "xlsx":
            df = _read_xlsx(spark, f)
        elif kind == "csv":
            df = _read_one_csv(spark, f, enc)
        elif kind == "json":
            df = spark.read.option("multiLine", cfg.get("multiline", True)).json(f)
        elif kind == "insee_fixe":
            df = _read_insee_fixe(spark, f, enc)
        else:
            raise ValueError(f"Format inconnu : {kind}")
        if df is not None:
            parts.append(apply_mapping(_with_lineage(df, F.lit(f)), cfg["columns"]))
    return reduce(lambda a, b: a.unionByName(b), parts)


def latest_by(df: DataFrame, keys: list[str]) -> DataFrame:
    """Garde, pour chaque clé, la ligne de l'ingestion la plus récente."""
    w = Window.partitionBy(*keys).orderBy(F.col("_ingest_date").desc_nulls_last(), F.col("_ligne").desc())
    return df.withColumn("_rk", F.row_number().over(w)).filter("_rk = 1").drop("_rk")


# ===========================================================================
# Normalisation
# ===========================================================================

_NULLS = ["", "NA", "N/A", "NULL", "NONE", "NAN", "-", "?"]


def clean(c) -> Column:
    """Texte nettoyé : espaces retirés, chaînes vides et 'N/A' -> NULL."""
    c = F.trim(F.col(c) if isinstance(c, str) else c)
    return F.when(c.isNull() | F.upper(c).isin(_NULLS), F.lit(None).cast("string")).otherwise(c)


def clean_len(c, n: int) -> Column:
    """Texte nettoyé et tronqué à n caractères (longueur du contrat)."""
    return F.substring(clean(c), 1, n)


def canon_id(c) -> Column:
    """Identifiant source canonique : '00123' et '123.0' -> forme stable."""
    c = clean(c)
    return F.regexp_replace(c, r"^(\d+)\.0+$", "$1")


_DATE_FORMATS = ["yyyy-MM-dd", "dd/MM/yyyy", "yyyyMMdd", "dd-MM-yyyy", "yyyy/MM/dd",
                 "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "d/M/yyyy"]


def parse_date(c, formats: list[str] | None = None) -> Column:
    """formats : formats propres à la source (config "date_formats"). S'ils sont
    donnés, seuls eux (+ ISO) sont essayés : un 4/6/1980 américain ne doit pas
    être relu en jour/mois par un format de repli."""
    c = clean(c)
    # Date ou timestamp déjà typé (Avro, Parquet) : relu en texte ISO "2015-01-01[ 00:00:00]"
    iso = F.when(c.rlike(r"^\d{4}-\d{2}-\d{2}([ T]|$)"),
                 F.to_date(F.try_to_timestamp(F.substring(c, 1, 10), F.lit("yyyy-MM-dd"))))
    fmts = formats or _DATE_FORMATS
    return F.coalesce(iso, *[F.to_date(F.try_to_timestamp(c, F.lit(f))) for f in fmts])


def date_formats(source: str) -> list[str] | None:
    return SOURCES[source].get("date_formats")


def parse_time_minutes(c) -> Column:
    """'08:30' ou '08:30:00' -> minutes depuis minuit."""
    c = clean(c)
    m = F.regexp_extract(c, r"^(\d{1,2})[:hH](\d{2})", 0)
    h = F.regexp_extract(c, r"^(\d{1,2})[:hH](\d{2})", 1).cast("int")
    mi = F.regexp_extract(c, r"^(\d{1,2})[:hH](\d{2})", 2).cast("int")
    return F.when(m != "", h * 60 + mi)


def duree_minutes(debut: Column, fin: Column) -> Column:
    """Durée en minutes ; une fin plus tôt que le début = passage de minuit
    (ex. 20:00 -> 02:00 = 360 min). Durée nulle -> NULL."""
    d = fin - debut
    d = F.when(d < 0, d + 1440).otherwise(d)
    return F.when(d > 0, d)


def to_long(c) -> Column:
    c = F.regexp_replace(clean(c), r"\.0+$", "")
    return F.when(c.rlike(r"^-?\d{1,18}$"), c.cast("bigint"))


def to_int(c) -> Column:
    c = F.regexp_replace(clean(c), r"\.0+$", "")
    return F.when(c.rlike(r"^-?\d{1,9}$"), c.cast("int"))


def to_double(c) -> Column:
    """Gère la virgule décimale française."""
    c = F.regexp_replace(F.regexp_replace(clean(c), " ", ""), ",", ".")
    return F.when(c.rlike(r"^-?\d+(\.\d+)?$"), c.cast("double"))


def norm_sexe(c) -> Column:
    c = F.upper(clean(c))
    return (F.when(c.isin("M", "H", "HOMME", "MASCULIN", "MALE", "1"), F.lit("M"))
             .when(c.isin("F", "FEMME", "FEMININ", "FÉMININ", "FEMALE", "2"), F.lit("F")))


def norm_insee(c) -> Column:
    """Code commune INSEE sur 5 caractères (zéros de tête, 2A/2B conservés)."""
    c = F.upper(F.regexp_replace(clean(c), r"\s", ""))
    c = F.when(c.rlike(r"^\d{4}$"), F.lpad(c, 5, "0")).otherwise(c)
    return F.when(c.rlike(r"^(\d{5}|2[AB]\d{3})$"), c)


def norm_finess(c) -> Column:
    """FINESS sur 9 caractères ; NULL si invalide (utiliser raw pour savoir s'il était renseigné)."""
    c = F.upper(F.regexp_replace(clean(c), r"[\s.\-]", ""))
    c = F.when(c.rlike(r"^\d{8}$"), F.lpad(c, 9, "0")).otherwise(c)
    return F.when(c.rlike(r"^(\d{9}|2[AB]\d{7})$"), c)


def norm_code_postal(c) -> Column:
    c = F.regexp_replace(clean(c), r"\s", "")
    c = F.when(c.rlike(r"^\d{4}$"), F.lpad(c, 5, "0")).otherwise(c)
    return F.when(c.rlike(r"^\d{5}$"), c)


def departement_from_cp(cp: Column) -> Column:
    """Code postal normalisé -> code département (Corse 2A/2B, outre-mer sur 3)."""
    return (F.when(cp.isNull(), None)
             .when(cp.startswith("20"), F.when(cp.cast("int") < 20200, "2A").otherwise("2B"))
             .when(cp.substr(1, 2).isin("97", "98"), cp.substr(1, 3))
             .otherwise(cp.substr(1, 2)))


def norm_cim10(c) -> Column:
    """Code CIM-10 : majuscules, sans point ni espace (J45.9 -> J459)."""
    c = F.upper(F.regexp_replace(clean(c), r"[\s.]", ""))
    return F.when(c.rlike(r"^[A-Z]\d{2}[0-9A-Z]{0,5}$"), c)


def age_at(birth: Column, event: Column) -> Column:
    a = F.floor(F.months_between(event, birth) / 12).cast("int")
    return F.when((a >= 0) & (a <= 120), a)


# Chapitres CIM-10 (OMS) : borne inférieure du chapitre -> (numéro, libellé)
CIM10_CHAPITRES = [
    ("A00", "I", "Certaines maladies infectieuses et parasitaires"),
    ("C00", "II", "Tumeurs"),
    ("D50", "III", "Maladies du sang et des organes hématopoïétiques et certains troubles du système immunitaire"),
    ("E00", "IV", "Maladies endocriniennes, nutritionnelles et métaboliques"),
    ("F00", "V", "Troubles mentaux et du comportement"),
    ("G00", "VI", "Maladies du système nerveux"),
    ("H00", "VII", "Maladies de l'œil et de ses annexes"),
    ("H60", "VIII", "Maladies de l'oreille et de l'apophyse mastoïde"),
    ("I00", "IX", "Maladies de l'appareil circulatoire"),
    ("J00", "X", "Maladies de l'appareil respiratoire"),
    ("K00", "XI", "Maladies de l'appareil digestif"),
    ("L00", "XII", "Maladies de la peau et du tissu cellulaire sous-cutané"),
    ("M00", "XIII", "Maladies du système ostéo-articulaire, des muscles et du tissu conjonctif"),
    ("N00", "XIV", "Maladies de l'appareil génito-urinaire"),
    ("O00", "XV", "Grossesse, accouchement et puerpéralité"),
    ("P00", "XVI", "Certaines affections dont l'origine se situe dans la période périnatale"),
    ("Q00", "XVII", "Malformations congénitales et anomalies chromosomiques"),
    ("R00", "XVIII", "Symptômes, signes et résultats anormaux d'examens cliniques et de laboratoire, non classés ailleurs"),
    ("S00", "XIX", "Lésions traumatiques, empoisonnements et certaines autres conséquences de causes externes"),
    ("U00", "XXII", "Codes d'utilisation particulière"),
    ("V01", "XX", "Causes externes de morbidité et de mortalité"),
    ("Z00", "XXI", "Facteurs influant sur l'état de santé et motifs de recours aux services de santé"),
]


def cim10_chapitre(code: Column) -> tuple[Column, Column]:
    """Renvoie (code_chapitre, libelle_chapitre) d'après les 3 premiers caractères."""
    p = code.substr(1, 3)
    num, lib = F.lit(None).cast("string"), F.lit(None).cast("string")
    for start, n, label in CIM10_CHAPITRES:  # dernier "start <= p" gagne
        num = F.when(p >= start, F.lit(n)).otherwise(num)
        lib = F.when(p >= start, F.lit(label)).otherwise(lib)
    return F.when(code.isNotNull(), num), F.when(code.isNotNull(), lib)


# ===========================================================================
# Pseudonymisation
# ===========================================================================

def hmac_key(settings: Settings) -> bytes:
    key = os.getenv(settings.hmac_key_env)
    if not key:
        raise RuntimeError(f"Clé de pseudonymisation absente : variable {settings.hmac_key_env} "
                           "(à injecter depuis OpenBao par Airflow)")
    return key.encode("utf-8")


def pseudo(c: Column, key: bytes) -> Column:
    """HMAC-SHA256 en hexadécimal (64 caractères). Même clé pour toutes les tables :
    un même identifiant donne toujours le même pseudonyme, les jointures marchent."""
    def _h(v):
        return None if v is None else hmac.new(key, v.encode("utf-8"), hashlib.sha256).hexdigest()
    return F.udf(_h, T.StringType())(c)


def pseudo_join(df: DataFrame, col_in: str, col_out: str, key: bytes) -> DataFrame:
    """Pseudonymise une colonne en ne calculant le HMAC qu'une fois par valeur
    distincte (1 M de consultations, mais 100 000 patients) puis jointure."""
    m = df.select(F.col(col_in).alias("_v")).dropna().distinct()
    m = m.withColumn(col_out, pseudo(F.col("_v"), key))
    return df.join(m, df[col_in] == m["_v"], "left").drop("_v")


# ===========================================================================
# Quarantaine et dédoublonnage
# ===========================================================================

QUARANTINE_COLS = ["table_silver", "categorie", "motif", "cle", "fichier", "ligne"]


class Quarantine:
    """Accumule les lignes écartées. Catégories :
    - rejet          : ligne non conforme, compte dans le seuil de 5 %
    - doublon        : version écartée d'un doublon ou d'une double saisie
    - hors_perimetre : ligne valide mais hors du périmètre (ex. décès à l'étranger)
    La quarantaine ne stocke PAS la ligne brute (elle contiendrait des données
    identifiantes) : seulement la clé (pseudonymisée si besoin), le fichier et
    le numéro de ligne, pour retrouver l'original en Bronze."""

    def __init__(self, table: str):
        self.table = table
        self.parts: list[DataFrame] = []

    def split(self, df: DataFrame, bad: Column, motif: str, categorie: str = "rejet") -> DataFrame:
        bad = F.coalesce(bad, F.lit(False))
        self.parts.append(df.filter(bad).select(
            F.lit(self.table).alias("table_silver"), F.lit(categorie).alias("categorie"),
            F.lit(motif).alias("motif"), F.col("_cle").cast("string").alias("cle"),
            F.col("_fichier").alias("fichier"), F.col("_ligne").cast("long").alias("ligne")))
        return df.filter(~bad)

    def frame(self, spark: SparkSession) -> DataFrame:
        if not self.parts:
            schema = "table_silver string, categorie string, motif string, cle string, fichier string, ligne long"
            return spark.createDataFrame([], schema)
        return reduce(lambda a, b: a.unionByName(b), self.parts)


def completeness(cols: list[str]) -> Column:
    return reduce(lambda a, b: a + b, [F.col(c).isNotNull().cast("int") for c in cols])


def dedupe(df: DataFrame, keys: list[str], value_cols: list[str], q: Quarantine,
           policy: str = "most_complete", latest_first: bool = False) -> DataFrame:
    """1) lignes strictement identiques : fusionnées (sans trace, Livrable 1) ;
    2) même clé, valeurs différentes (conflit) :
       - policy="reject"        : toutes les versions en quarantaine (rejet)
       - policy="most_complete" : la plus complète est gardée, les autres en doublon
       latest_first=True : la plus récente (ingestion) prime sur la complétude."""
    df = df.dropDuplicates(keys + [c for c in value_cols if c not in keys])
    n = F.count(F.lit(1)).over(Window.partitionBy(*keys))
    df = df.withColumn("_n", n)
    if policy == "reject":
        return q.split(df, F.col("_n") > 1, "conflit de clé (valeurs différentes)").drop("_n")
    order = [F.col("_ingest_date").desc_nulls_last()] if latest_first else []
    order += [completeness(value_cols).desc(), F.col("_ingest_date").desc_nulls_last(), F.col("_ligne").desc()]
    df = df.withColumn("_rk", F.row_number().over(Window.partitionBy(*keys).orderBy(*order)))
    df = q.split(df, F.col("_rk") > 1, "conflit de clé : version écartée", "doublon")
    return df.drop("_n", "_rk")
