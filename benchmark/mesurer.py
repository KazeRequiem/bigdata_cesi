#!/usr/bin/env python3
"""Mesure les temps de réponse des requêtes B1 à B8 dans trois configurations et range les résultats
dans gold.bench_resultats (lu par l'onglet « Performance » du tableau de bord Direction).

    docker compose exec superset python /app/benchmark/mesurer.py [--executions 5]

Configurations :
  Avant (gold_v0)   requêtes de benchmark/requetes/ avec search_path = gold_v0, gold
                    (faits ni partitionnés ni indexés)
  Après (gold)      mêmes requêtes avec search_path = gold (partitions annuelles + index)
  Vue BI (gold.mv_*) même indicateur lu dans les vues dédiées au tableau de bord (sql/13)
Protocole : 1 exécution de chauffe (non comptée) puis N exécutions ; on garde la médiane.
Les paramètres (période, établissement, diagnostic...) viennent de benchmark/requetes/parametres.sql.
Lancé dans le conteneur Superset (seul conteneur avec Python et psycopg2) avec le compte administrateur
de la base : ne rien lancer d'autre pendant la mesure.
"""
import argparse
import os
import re
import statistics
import time
from datetime import datetime
from pathlib import Path

import psycopg2

ICI = Path(__file__).resolve().parent
REQUETES = ICI / "requetes"

LIBELLES = {
    "B1": "Consultations d'un établissement",
    "B2": "Consultations d'un diagnostic",
    "B3": "Taux global d'hospitalisation",
    "B4": "Hospitalisations par diagnostic",
    "B5": "Hospitalisations par sexe et âge",
    "B6": "Consultations par professionnel",
    "B7": "Décès par région (2019)",
    "B8": "Satisfaction par région (2020)",
}

# Même indicateur, lu dans les vues du tableau de bord (mêmes paramètres psql)
REQUETES_VUES_BI = {
    "B1": """SELECT etablissement, count(*) AS nb_consultations,
       round(100.0 * count(*) / (SELECT count(*) FROM gold.mv_consultations
            WHERE date_consultation BETWEEN to_date(:debut::text, 'YYYYMMDD') AND to_date(:fin::text, 'YYYYMMDD')), 3)
FROM gold.mv_consultations
WHERE sk_etablissement IN (SELECT sk_etablissement FROM gold.dim_etablissement WHERE id_organisation = :'organisation')
  AND date_consultation BETWEEN to_date(:debut::text, 'YYYYMMDD') AND to_date(:fin::text, 'YYYYMMDD')
GROUP BY etablissement""",
    "B2": """SELECT diagnostic, count(*) AS nb_consultations,
       round(100.0 * count(*) / (SELECT count(*) FROM gold.mv_consultations
            WHERE date_consultation BETWEEN to_date(:debut::text, 'YYYYMMDD') AND to_date(:fin::text, 'YYYYMMDD')), 3)
FROM gold.mv_consultations
WHERE code_diag = :'code_diag'
  AND date_consultation BETWEEN to_date(:debut::text, 'YYYYMMDD') AND to_date(:fin::text, 'YYYYMMDD')
GROUP BY diagnostic""",
    "B3": """SELECT sum(patients_hospitalises), sum(patients_pris_en_charge),
       round(100.0 * sum(patients_hospitalises) / NULLIF(sum(patients_pris_en_charge), 0), 2)
FROM gold.mv_taux_hospitalisation WHERE annee = :debut / 10000""",
    "B4": """SELECT code_diag, diagnostic, count(*) AS nb_sejours,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2)
FROM gold.mv_hospitalisations
WHERE date_entree BETWEEN to_date(:debut::text, 'YYYYMMDD') AND to_date(:fin::text, 'YYYYMMDD')
GROUP BY code_diag, diagnostic ORDER BY nb_sejours DESC LIMIT 20""",
    "B5": """SELECT sexe, tranche_age, count(*) AS nb_sejours, round(100.0 * count(*) / sum(count(*)) OVER (), 2)
FROM gold.mv_hospitalisations
WHERE date_entree BETWEEN to_date(:debut::text, 'YYYYMMDD') AND to_date(:fin::text, 'YYYYMMDD')
GROUP BY 1, 2 ORDER BY 1, 2""",
    "B6": """SELECT id_professionnel, specialite, count(*) AS nb_consultations,
       round(100.0 * count(*) / sum(count(*)) OVER (), 3)
FROM gold.mv_consultations_pro WHERE annee = :debut / 10000
GROUP BY id_professionnel, specialite ORDER BY nb_consultations DESC LIMIT 20""",
    "B7": """SELECT code_region, region, sum(nb_deces) AS nb_deces
FROM gold.mv_deces WHERE annee = :annee_deces
GROUP BY code_region, region ORDER BY nb_deces DESC""",
    "B8": """SELECT code_region, region, count(DISTINCT finess_site), sum(nb_reponses),
       round(sum(score_global * nb_reponses) / NULLIF(sum(nb_reponses), 0), 2) AS score_pondere
FROM gold.v_satisfaction WHERE annee = :annee_satisfaction
GROUP BY code_region, region ORDER BY score_pondere DESC""",
}


def lire_parametres():
    params = {}
    for ligne in (REQUETES / "parametres.sql").read_text(encoding="utf-8").splitlines():
        r = re.match(r"\\set\s+(\w+)\s+(.+?)\s*$", ligne)
        if r:
            params[r.group(1)] = r.group(2).strip("'")
    return params


def substituer(sql, params):
    """Remplace les variables psql (:'nom' -> littéral texte, :nom -> valeur brute) sans toucher aux ::casts."""
    sql = re.sub(r":'(\w+)'", lambda r: "'" + params[r.group(1)].replace("'", "''") + "'", sql)
    return re.sub(r"(?<![:\w]):(\w+)\b", lambda r: params[r.group(1)] if r.group(1) in params else r.group(0), sql)


def mesurer(cur, sql, n):
    cur.execute(sql)            # chauffe : met les pages en cache
    lignes = len(cur.fetchall())
    temps = []
    for _ in range(n):
        t0 = time.perf_counter()
        cur.execute(sql)
        cur.fetchall()
        temps.append((time.perf_counter() - t0) * 1000)
    return statistics.median(temps), min(temps), max(temps), lignes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--executions", type=int, default=5)
    n = ap.parse_args().executions
    params = lire_parametres()
    cx = psycopg2.connect(host=os.environ.get("POSTGRES_HOST", "postgres"), dbname=os.environ["POSTGRES_DB"],
                          user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"])
    cx.autocommit = True
    cur = cx.cursor()
    resultats = []
    for fichier in sorted(REQUETES.glob("B*.sql")):
        code = fichier.name[:2]
        sql = substituer(fichier.read_text(encoding="utf-8"), params)
        configs = [("Avant (gold_v0)", "SET search_path TO gold_v0, gold", sql),
                   ("Après (gold)", "SET search_path TO gold", sql)]
        if code in REQUETES_VUES_BI:
            configs.append(("Vue BI (gold.mv_*)", "SET search_path TO gold", substituer(REQUETES_VUES_BI[code], params)))
        for nom, chemin, requete in configs:
            cur.execute(chemin)
            med, mn, mx, lignes = mesurer(cur, requete, n)
            resultats.append((code, LIBELLES.get(code, code), nom, med, mn, mx, n, lignes))
            print(f"{code} {nom:<20} médiane {med:9.1f} ms  (min {mn:.1f}, max {mx:.1f}, {lignes} lignes)")
    cur.execute("SET search_path TO gold")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gold.bench_resultats (
            requete        VARCHAR(4)    NOT NULL,
            libelle        VARCHAR(80)   NOT NULL,
            configuration  VARCHAR(30)   NOT NULL,
            mediane_ms     NUMERIC(10,2) NOT NULL,
            min_ms         NUMERIC(10,2) NOT NULL,
            max_ms         NUMERIC(10,2) NOT NULL,
            nb_executions  SMALLINT      NOT NULL,
            nb_lignes      INTEGER       NOT NULL,
            date_mesure    TIMESTAMP     NOT NULL,
            PRIMARY KEY (requete, configuration))""")
    cur.execute("TRUNCATE gold.bench_resultats")
    maintenant = datetime.now()
    cur.executemany("INSERT INTO gold.bench_resultats VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    [r + (maintenant,) for r in resultats])
    print(f"{len(resultats)} mesures rangées dans gold.bench_resultats")


if __name__ == "__main__":
    main()
