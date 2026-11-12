"""Chaîne CHU : attente de Bronze -> Silver (Spark/Iceberg) -> staging -> Gold (PostgreSQL) -> qualité.

Les tâches pilotent les conteneurs existants avec `docker exec` (le socket Docker est monté dans airflow-scheduler) :
  chu-spark     -> silver ddl | run --job <table> | export | qualite   (les secrets viennent d'OpenBao, pas d'Airflow)
  chu-postgres  -> psql -f /sql/08, 09, 10, 14
Ordre de l'architecture (Livrable 1) : S2 (établissements) d'abord, puis les autres sources ; chaque job Silver
attend ses référentiels. Déclenchement manuel (schedule=None) ; pour un lot quotidien : schedule="0 2 * * *".
"""
import json
import os
import urllib.request
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

FILER = os.getenv("BRONZE_FILER_URL", "http://seaweedfs:8888")
PG_USER = os.getenv("POSTGRES_USER", "chu")
PG_DB = os.getenv("POSTGRES_DB", "chu_dw")

# ordre imposé par les dépendances : référentiels, entités, faits
JOBS = ["communes", "etablissements", "professionnels", "diagnostics", "patients",
        "consultations", "hospitalisations", "satisfaction", "deces"]
# dossiers Bronze à attendre : S2 d'abord (référentiel des établissements)
SOURCES_ORDRE = ["s2_etablissements", "s0_cog", "s1_soins", "s1_hospitalisations", "s3_satisfaction", "s4_deces"]


def alerte(context):
    """Appelée à l'échec définitif d'une tâche. Webhook facultatif : variable ALERT_WEBHOOK_URL."""
    ti = context["task_instance"]
    msg = f"[CHU] échec de {ti.dag_id}.{ti.task_id} (essai {ti.try_number}, exécution {ti.run_id})"
    print("ALERTE " + msg)
    url = os.getenv("ALERT_WEBHOOK_URL")
    if url:
        req = urllib.request.Request(url, data=json.dumps({"text": msg}).encode(),
                                     headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:  # l'alerte ne doit jamais masquer l'échec d'origine
            print(f"webhook injoignable : {e}")


def _ls(path):
    req = urllib.request.Request(f"{FILER}{path}?limit=1000", headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r).get("Entries") or []


def manifeste_present(source):
    """Vrai si Bronze contient au moins un manifeste (<fichier>.manifest.json) dans bronze/<source>/<date>/."""
    try:
        for d in _ls(f"/buckets/bronze/{source}/"):
            for f in _ls(d["FullPath"] + "/"):
                if f["FullPath"].endswith(".manifest.json"):
                    return True
    except Exception as e:
        print(f"bronze/{source} : {e}")
    return False


def silver(args):
    return f"docker exec chu-spark silver {args}"


def psql(script):
    return f"docker exec chu-postgres psql -v ON_ERROR_STOP=1 -U {PG_USER} -d {PG_DB} -f /sql/{script}"


with DAG(
    dag_id="chu_pipeline",
    description="Bronze -> Silver -> Gold -> qualité",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,          # Spark (4 Go) : une tâche à la fois
    default_args={
        "owner": "chu",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "on_failure_callback": alerte,
    },
    tags=["chu", "lakehouse"],
) as dag:

    attentes = []
    for src in SOURCES_ORDRE:
        attentes.append(PythonSensor(
            task_id=f"attendre_{src}",
            python_callable=manifeste_present, op_args=[src],
            poke_interval=30, timeout=600, mode="reschedule", retries=0,
        ))
    for a, b in zip(attentes, attentes[1:]):
        a >> b

    ddl = BashOperator(task_id="silver_ddl", bash_command=silver("ddl"))
    attentes[-1] >> ddl

    precedent = ddl
    for job in JOBS:
        t = BashOperator(task_id=f"silver_{job}", bash_command=silver(f"run --job {job}"))
        precedent >> t
        precedent = t

    export = BashOperator(task_id="export_staging", bash_command=silver("export --tables all"))
    dims = BashOperator(task_id="gold_dimensions", bash_command=psql("08_charger_dimensions.sql"))
    faits = BashOperator(task_id="gold_faits", bash_command=psql("09_charger_faits.sql"))
    v0 = BashOperator(task_id="gold_copie_v0", bash_command=psql("10_copier_v0.sql"))
    # vues matérialisées des tableaux de bord Superset (Livrable 3), recalculées après chaque chargement
    vues_bi = BashOperator(task_id="gold_vues_dashboard", bash_command=psql("14_rafraichir_vues_dashboard.sql"))
    qualite = BashOperator(task_id="rapport_qualite",
                           bash_command=silver("qualite --out /opt/silver/docs/rapport_qualite_airflow.md"))
    precedent >> export >> dims >> faits >> v0 >> vues_bi >> qualite
