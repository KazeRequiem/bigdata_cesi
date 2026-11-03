"""Tests bout en bout Bronze -> Silver sur le jeu fictif (mode parquet, sans Iceberg)."""
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("SILVER_HMAC_KEY", "cle-de-test-ne-pas-utiliser-en-production")

from pyspark.sql import functions as F  # noqa: E402

from silver import contract  # noqa: E402
from silver.common import get_spark  # noqa: E402
from silver.config import Settings  # noqa: E402
from silver.context import Ctx  # noqa: E402
from silver.jobs import JOBS, ORDER  # noqa: E402
from silver.quality_report import build_report  # noqa: E402
from silver.run import RejectThresholdExceeded, run_job  # noqa: E402
from silver.store import SilverStore  # noqa: E402

import fixtures_bronze  # noqa: E402

HEX64 = re.compile(r"^[0-9a-f]{64}$")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    SparkSession.builder.master("local[2]").config("spark.ui.showConsoleProgress", "false") \
        .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false").getOrCreate()
    s = get_spark("silver-tests")
    s.sparkContext.setLogLevel("ERROR")
    return s


def _ctx(spark, tmp, threshold=0.9, run="r1"):
    # Seuil relevé : le jeu fictif est minuscule et volontairement sale.
    st = Settings(bronze_root=str(tmp / "bronze"), storage="parquet",
                  local_root=str(tmp / "silver"), reject_threshold=threshold)
    return Ctx(spark, st, SilverStore(spark, st), run_id=run)


@pytest.fixture(scope="session")
def silver(spark, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("lake")
    fixtures_bronze.build(spark, str(tmp / "bronze"))
    ctx = _ctx(spark, tmp)
    stats = {m.TABLE: run_job(ctx, m) for m in ORDER}
    return ctx, stats, tmp


def rows(ctx, table, order):
    return [r.asDict() for r in ctx.store.read(table).orderBy(order).collect()]


def quarantine(ctx, table):
    return {(r["categorie"], r["motif"]) for r in
            ctx.store.read("quarantaine").filter(F.col("table_silver") == table).collect()}


# ---------------------------------------------------------------- contrat
def test_schemas_respectent_le_contrat(silver):
    ctx, stats, _ = silver
    allowed_tech = {"_fichier", "_ingest_date", "_run_id", "_id_deces"}
    for table in contract.CONTRACT:
        df = ctx.store.read(table)
        business = [c for c in df.columns if not c.startswith("_")]
        assert business == contract.columns(table), table
        assert set(c for c in df.columns if c.startswith("_")) <= allowed_tech, table
        assert stats[table]["statut"] == "OK"


def test_aucun_identifiant_en_clair(silver):
    ctx, _, _ = silver
    for t in ["patients", "consultations", "hospitalisations"]:
        for r in ctx.store.read(t).select("id_patient_pseudo").collect():
            assert HEX64.match(r[0])
    for r in ctx.store.read("professionnels").select("id_professionnel").collect():
        assert HEX64.match(r[0])
    # La quarantaine ne contient que des pseudonymes pour les personnes
    for r in ctx.store.read("quarantaine").filter("table_silver = 'patients'").collect():
        assert r["cle"] is None or HEX64.match(r["cle"])


# --------------------------------------------------------------- communes
def test_communes(silver):
    ctx, _, _ = silver
    r = {x["code_commune_insee"]: x for x in rows(ctx, "communes", "code_commune_insee")}
    assert set(r) == {"01001", "2A004", "59350", "62041", "75056", "75101"}
    assert (r["75101"]["code_departement"], r["75101"]["libelle_region"]) == ("75", "Île-de-France")
    assert r["2A004"]["libelle_departement"] == "Corse-du-Sud"
    assert ("rejet", "code commune invalide") in quarantine(ctx, "communes")
    passage = {x["code_ancien"]: x["code_actuel"] for x in ctx.store.read("passage_communes").collect()}
    assert passage == {"59355": "59350", "62999": "62041"}


# --------------------------------------------------------- établissements
def test_etablissements(silver):
    ctx, _, _ = silver
    r = {x["id_organisation"]: x for x in rows(ctx, "etablissements", "id_organisation")}
    assert set(r) == {"F590780193", "F620100057", "F2A0000013", "F05907801901012007", "R10100000139221"}
    assert r["F590780193"]["finess_site"] == "590780193"   # FINESS déduit de l'identifiant "F..."
    assert r["F620100057"]["siret"] == "26620111100011"    # version la plus complète gardée
    assert r["F2A0000013"]["finess_site"] == "2A0000013"
    lomme = r["F05907801901012007"]                         # variante F + FINESS + date
    assert (lomme["finess_site"], lomme["code_commune_insee"], lomme["siret"]) == ("059078019", "59350", None)
    assert r["R10100000139221"]["finess_site"] is None      # cabinet : pas de FINESS
    assert str(r["F590780193"]["date_extraction"]) == "2026-11-01"
    q = quarantine(ctx, "etablissements")
    assert ("rejet", "FINESS site invalide (9 caractères attendus)") in q
    assert ("rejet", "id_organisation absent") in q
    assert ("doublon", "conflit de clé : version écartée") in q


# ----------------------------------------------- professionnels / diagnostics
def test_professionnels_et_diagnostics(silver):
    ctx, _, _ = silver
    p = ctx.store.read("professionnels")
    assert p.count() == 3
    assert {r[0] for r in p.select("type_identifiant").collect()} == {"RPPS", "ADELI"}
    spe = {(r["code_specialite"], r["libelle_specialite"], r["fonction"]) for r in p.collect()}
    assert ("PNE01", "Pneumologie", "Medecin") in spe and (None, None, None) in spe
    d = {x["code_diag"]: x for x in rows(ctx, "diagnostics", "code_diag")}
    assert set(d) == {"C50", "I10", "J459"}
    assert (d["J459"]["code_chapitre"], d["C50"]["code_chapitre"], d["I10"]["code_chapitre"]) == ("X", "II", "IX")


# ---------------------------------------------------------------- patients
def test_patients(silver):
    ctx, _, _ = silver
    from silver.common import pseudo  # noqa
    df = ctx.store.read("patients")
    assert df.count() == 4
    by_dep = {(r["annee_naissance"], r["code_departement"], r["sexe"]) for r in df.collect()}
    assert by_dep == {(1980, "59", "F"), (1950, "2A", "M"), (2010, None, "F"), (None, "01", "M")}


# ----------------------------------------------------------- consultations
def test_consultations(silver):
    ctx, _, _ = silver
    r = {x["id_consultation"]: x for x in rows(ctx, "consultations", "id_consultation")}
    assert set(r) == {1, 2, 3, 10}
    assert (r[1]["id_organisation"], r[1]["age_consultation"], r[1]["duree_minutes"], r[1]["nb_prescriptions"]) \
        == ("F590780193", 39, 30, 2)                        # naissance 5/10/1980 = 10 mai (format M/d/yyyy)
    assert r[2]["id_organisation"] == "F620100057"          # établissement prioritaire sur le cabinet C...
    assert (r[2]["duree_minutes"], r[2]["age_consultation"]) == (360, 70)  # 20:00 -> 02:00
    assert r[3]["id_organisation"] == "R10100000139221"     # cabinet
    assert r[3]["code_diag"] is None and r[3]["duree_minutes"] is None
    assert r[10]["id_professionnel"] is None and r[10]["id_organisation"] is None and r[10]["nb_prescriptions"] == 0
    q = quarantine(ctx, "consultations")
    for motif in ["patient inexistant", "date de consultation absente ou invalide",
                  "conflit de clé (valeurs différentes)",
                  "date de consultation dans le futur"]:
        assert ("rejet", motif) in q, motif
    assert ("doublon", "double saisie (même patient, date, motif, professionnel)") in q


# -------------------------------------------------------- hospitalisations
def test_hospitalisations(silver):
    ctx, _, _ = silver
    r = {x["id_hospitalisation"]: x for x in rows(ctx, "hospitalisations", "id_hospitalisation")}
    assert set(r) == {100, 101, 103, 106}
    assert (str(r[100]["date_sortie"]), r[100]["duree_sejour_jours"], r[100]["id_organisation"]) \
        == ("2020-04-06", 5, "F590780193")
    assert (r[101]["duree_sejour_jours"], r[101]["id_organisation"]) == (2, "F05907801901012007")
    assert r[103]["id_organisation"] == "R10100000139221"   # cabinet : conservé
    assert r[106]["id_organisation"] is None                # organisation inconnue -> NULL, non bloquant
    q = {m for _, m in quarantine(ctx, "hospitalisations")}
    assert q == {"durée de séjour négative", "durée de séjour impossible à déterminer", "patient inexistant"}


# ------------------------------------------------------------ satisfaction
def test_satisfaction(silver):
    ctx, _, _ = silver
    r = {(x["finess_site"], x["annee"], x["type_enquete"]): x for x in rows(ctx, "satisfaction", "finess_site")}
    assert set(r) == {("590780193", 2019, "ESATIS48H_MCO"), ("059078019", 2019, "ESATIS48H_MCO"),
                      ("590780193", 2020, "ESATISCA_MCO"), ("059078019", 2020, "ESATISCA_MCO")}
    a = r[("590780193", 2019, "ESATIS48H_MCO")]
    assert (float(a["score_global"]), float(a["score_pec_medical"]), a["classement"]) == (75.5, 79.2, "A")
    assert r[("059078019", 2019, "ESATIS48H_MCO")]["score_global"] is None
    ca = r[("590780193", 2020, "ESATISCA_MCO")]            # lu depuis le .xlsx
    assert (float(ca["score_global"]), float(ca["score_accueil"]), float(ca["score_chambre"]),
            float(ca["score_sortie"]), ca["score_pec_medical"]) == (76.56, 79.53, 69.29, 68.27, None)
    q = quarantine(ctx, "satisfaction")
    assert ("rejet", "score hors de la plage 0-100") in q
    assert ("rejet", "classement inconnu") in q
    assert ("rejet", "score global vide avec un classement A à D") in q
    assert ("hors_perimetre", "FINESS introuvable dans le référentiel établissements") in q


# ------------------------------------------------------------------- décès
def test_deces(silver):
    ctx, stats, _ = silver
    r = rows(ctx, "deces", "_id_deces")
    assert len(r) == 5
    got = {(str(x["date_deces"]), x["code_commune_deces"], x["sexe"], x["age_deces"]) for x in r}
    assert got == {("2019-03-15", "59350", "M", 79), ("2019-01-01", "59350", "F", 89),   # 00/00/2019 -> 1er janvier
                   ("2019-07-10", "75101", "F", 74), ("2019-07-10", None, "M", 74),
                   ("2019-08-20", "01001", "F", 69)}
    assert stats["deces"]["hors_perimetre"] == 1


# --------------------------------------------------- rejeu / seuil / rapport
def test_rejeu_sans_doublon(spark, silver):
    ctx, _, tmp = silver
    before = {t: ctx.store.read(t).count() for t in contract.CONTRACT}
    ctx2 = _ctx(spark, tmp, run="r2")
    for m in ORDER:
        run_job(ctx2, m)
    after = {t: ctx2.store.read(t).count() for t in contract.CONTRACT}
    assert before == after


def test_seuil_de_rejet_bloque_le_job(spark, tmp_path):
    fixtures_bronze.build(spark, str(tmp_path / "bronze"))
    ctx = _ctx(spark, tmp_path, threshold=0.05)
    with pytest.raises(RejectThresholdExceeded):
        run_job(ctx, JOBS["communes"])          # 1 rejet sur 7 lignes > 5 %
    assert ctx.store.read("communes") is None   # Silver non modifié
    j = ctx.store.read("journal_executions").collect()
    assert j[0]["statut"] == "ECHEC"


def test_rapport_qualite(silver):
    ctx, _, tmp = silver
    report = build_report(ctx.store)
    assert "## 3. Cohérence référentielle" in report
    assert "organisation consultation -> établissement" in report
    (tmp / "rapport_qualite.md").write_text(report, encoding="utf-8")


def test_manifeste_falsifie_refuse(spark, tmp_path):
    """Contrôle d'entrée : un fichier dont l'empreinte ne correspond pas au
    manifeste NiFi est refusé en entier."""
    fixtures_bronze.build(spark, str(tmp_path / "bronze"))
    f = tmp_path / "bronze" / "s1_hospitalisations" / fixtures_bronze.D / "Hospitalisations.csv"
    fixtures_bronze.manifest(str(f), "s1_hospitalisations", sha256="0" * 64)
    ctx = _ctx(spark, tmp_path)
    with pytest.raises(ValueError, match="empreinte SHA-256"):
        ctx.bronze("s1_hospitalisation")
