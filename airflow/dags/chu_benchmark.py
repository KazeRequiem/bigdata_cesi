"""Relance du benchmark (onglet « Performance de la plateforme » du tableau de bord Direction).

Bouton : Airflow (http://localhost:8080) -> DAG chu_benchmark -> Trigger. Durée : environ 2 minutes.
  1. mesurer            B1 à B8 sur gold_v0 (avant), gold (après partitions + index) et les vues gold.mv_*
                        -> table gold.bench_resultats (médiane de 5 exécutions, benchmark/mesurer.py)
  2. maj_tableaux       relance docker/superset/dashboards/construire_dashboards.py pour recalculer le bandeau
                        « À retenir » de l'onglet (les graphiques de l'onglet ont un cache court, 60 s)

Comme chu_pipeline, les tâches pilotent un conteneur existant par `docker exec` (ici chu-superset, seul
conteneur avec Python et psycopg2) ; Airflow ne détient aucun mot de passe.
Ne pas lancer pendant chu_pipeline ni pendant un autre traitement lourd : les temps mesurés seraient faussés.
"""
import json
import os
import urllib.request
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


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


with DAG(
    dag_id="chu_benchmark",
    description="Mesure avant / après optimisation (B1 à B8) et mise à jour de l'onglet Performance de Superset",
    doc_md=__doc__,
    start_date=datetime(2026, 1, 1),
    schedule=None,                 # déclenchement manuel uniquement (bouton Trigger)
    catchup=False,
    max_active_runs=1,             # deux mesures en parallèle se fausseraient mutuellement
    max_active_tasks=1,
    is_paused_upon_creation=False, # le bouton Trigger fonctionne dès l'apparition du DAG
    default_args={
        "owner": "chu",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": alerte,
    },
    tags=["chu", "benchmark"],
) as dag:

    mesurer = BashOperator(
        task_id="mesurer",
        bash_command="docker exec chu-superset python /app/benchmark/mesurer.py",
        execution_timeout=timedelta(minutes=15),
    )

    maj_tableaux = BashOperator(
        task_id="maj_tableaux",
        bash_command="docker exec chu-superset python /app/dashboards/construire_dashboards.py",
        execution_timeout=timedelta(minutes=10),
    )

    mesurer >> maj_tableaux
