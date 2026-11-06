"""Rapport de qualité des données Silver (attendu du Livrable 2) :
- taux de complétude par attribut ;
- unicité des clés (doublons restants, doit être 0) ;
- cohérence référentielle (FINESS, CIM-10, patients, professionnels, communes) ;
- bilan de la quarantaine par table, catégorie et motif ;
- dernier passage de chaque job (journal).

    python -m silver.quality_report --out docs/rapport_qualite.md
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F

from . import contract
from .common import get_spark
from .config import Settings
from .store import SilverStore

# (table, colonne, table de référence, colonne de référence, libellé)
REFERENCES = [
    ("consultations", "id_organisation", "etablissements", "id_organisation", "organisation consultation -> établissement"),
    ("hospitalisations", "id_organisation", "etablissements", "id_organisation", "organisation hospitalisation -> établissement"),
    ("satisfaction", "finess_site", "etablissements", "finess_site", "FINESS satisfaction -> établissement"),
    ("consultations", "code_diag", "diagnostics", "code_diag", "CIM-10 consultation -> diagnostics"),
    ("hospitalisations", "code_diag", "diagnostics", "code_diag", "CIM-10 hospitalisation -> diagnostics"),
    ("consultations", "id_patient_pseudo", "patients", "id_patient_pseudo", "patient consultation -> patients"),
    ("hospitalisations", "id_patient_pseudo", "patients", "id_patient_pseudo", "patient hospitalisation -> patients"),
    ("consultations", "id_professionnel", "professionnels", "id_professionnel", "professionnel consultation -> professionnels"),
    ("etablissements", "code_commune_insee", "communes", "code_commune_insee", "commune établissement -> COG"),
    ("deces", "code_commune_deces", "communes", "code_commune_insee", "commune décès -> COG"),
]


def _pct(a, b):
    return f"{100 * a / b:.1f} %" if b else "-"


def build_report(store: SilverStore) -> str:
    out = ["# Rapport de qualité — couche Silver", "",
           f"Généré le {datetime.now():%Y-%m-%d %H:%M}.", ""]

    # 1. Volumes et complétude
    out += ["## 1. Complétude par attribut", "",
            "Part des lignes où l'attribut est renseigné (non NULL).", ""]
    tables = {}
    for t in contract.CONTRACT:
        df = store.read(t)
        if df is None:
            out += [f"### {t}", "", "_Table absente._", ""]
            continue
        tables[t] = df.cache()
        cols = contract.columns(t)
        row = df.agg(F.count(F.lit(1)).alias("_n"), *[F.count(c).alias(c) for c in cols]).first()
        n = row["_n"]
        out += [f"### {t} ({n} lignes)", "", "| Attribut | Renseigné | Complétude |", "|---|---:|---:|"]
        out += [f"| {c} | {row[c]} | {_pct(row[c], n)} |" for c in cols]
        out.append("")

    # 2. Unicité
    out += ["## 2. Unicité des clés", "", "| Table | Clé | Lignes | Clés en double |", "|---|---|---:|---:|"]
    for t, df in tables.items():
        k = contract.key(t)
        if not all(c in df.columns for c in k):
            continue
        n = df.count()
        dup = df.groupBy(*k).count().filter("count > 1").count()
        out.append(f"| {t} | {', '.join(k)} | {n} | {dup} |")
    out.append("")

    # 3. Cohérence référentielle
    out += ["## 3. Cohérence référentielle", "",
            "Parmi les valeurs renseignées, part qui existe dans la table de référence. "
            "Les valeurs NULL (inconnues) sont comptées à part.", "",
            "| Contrôle | Renseignées | Trouvées | Cohérence | NULL |", "|---|---:|---:|---:|---:|"]
    for t, c, rt, rc, label in REFERENCES:
        if t not in tables or rt not in tables:
            continue
        df, ref = tables[t], tables[rt].select(F.col(rc).alias("_r")).dropna().distinct()
        filled = df.filter(F.col(c).isNotNull())
        nf, nn = filled.count(), df.filter(F.col(c).isNull()).count()
        found = filled.join(ref, filled[c] == ref["_r"], "left_semi").count()
        out.append(f"| {label} | {nf} | {found} | {_pct(found, nf)} | {nn} |")
    if "etablissements" in tables:
        e = tables["etablissements"]
        bad = e.filter(F.col("finess_site").isNotNull() & ~F.col("finess_site").rlike(r"^(\d{9}|2[AB]\d{7})$")).count()
        out.append(f"| Format FINESS site (9 caractères) dans établissements | {e.filter('finess_site IS NOT NULL').count()} "
                   f"| - | {bad} invalide(s) | {e.filter('finess_site IS NULL').count()} |")
    out.append("")

    # 4. Quarantaine
    q = store.read("quarantaine")
    j = store.read("journal_executions")
    if q is not None and j is not None:
        # Seulement la dernière exécution de chaque table (les rejeux s'empilent)
        last_run = (j.withColumn("_rk", F.row_number().over(
            Window.partitionBy("table_silver").orderBy(F.desc("debut"))))
            .filter("_rk = 1").select("table_silver", "run_id"))
        q = q.join(last_run, ["table_silver", "run_id"], "inner")
    out += ["## 4. Quarantaine (dernière exécution de chaque table)", ""]
    if q is None:
        out += ["_Aucune ligne en quarantaine._", ""]
    else:
        out += ["| Table | Catégorie | Motif | Lignes |", "|---|---|---|---:|"]
        for r in q.groupBy("table_silver", "categorie", "motif").count() \
                  .orderBy("table_silver", "categorie", F.desc("count")).collect():
            out.append(f"| {r['table_silver']} | {r['categorie']} | {r['motif']} | {r['count']} |")
        out.append("")

    # 5. Journal
    if j is not None:
        out += ["## 5. Dernière exécution par table", "",
                "| Table | Statut | Lues | Écrites | Rejets | Taux de rejet | Doublons | Hors périmètre |",
                "|---|---|---:|---:|---:|---:|---:|---:|"]
        last = j.withColumn("_rk", F.row_number().over(
            Window.partitionBy("table_silver").orderBy(F.desc("debut"))))
        for r in last.filter("_rk = 1").orderBy("table_silver").collect():
            out.append(f"| {r['table_silver']} | {r['statut']} | {r['lignes_lues']} | {r['lignes_ecrites']} | "
                       f"{r['rejets']} | {_pct(r['rejets'] or 0, r['lignes_lues'] or 0)} | {r['doublons']} | "
                       f"{r['hors_perimetre']} |")
        out.append("")
    return "\n".join(out)


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="docs/rapport_qualite.md")
    a = p.parse_args(argv)
    spark, settings = get_spark("silver-quality"), Settings()
    report = build_report(SilverStore(spark, settings))
    with open(a.out, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"Rapport écrit : {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
