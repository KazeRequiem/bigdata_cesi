"""Prépare Airflow (service airflow-init, rejouable) :
 1. crée le rôle et la base "airflow" dans le PostgreSQL du projet (compte administrateur, une seule fois) ;
 2. écrit le fichier des comptes de l'interface (SimpleAuthManager) ;
 3. applique les migrations de la base de métadonnées.
Lit POSTGRES_HOST/PORT/USER/PASSWORD (admin), AIRFLOW_DB_PASSWORD, AIRFLOW_ADMIN_USER/PASSWORD."""
import json
import os
import subprocess
import sys

import psycopg2
from psycopg2 import sql

env = os.environ
conn = psycopg2.connect(host=env["POSTGRES_HOST"], port=5432, user=env["POSTGRES_USER"],
                        password=env["POSTGRES_PASSWORD"], dbname="postgres")
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'airflow'")
if cur.fetchone():
    cur.execute(sql.SQL("ALTER ROLE airflow LOGIN PASSWORD {}").format(sql.Literal(env["AIRFLOW_DB_PASSWORD"])))
else:
    cur.execute(sql.SQL("CREATE ROLE airflow LOGIN PASSWORD {}").format(sql.Literal(env["AIRFLOW_DB_PASSWORD"])))
cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
if not cur.fetchone():
    cur.execute("CREATE DATABASE airflow OWNER airflow")
conn.close()
print("[airflow-init] base de métadonnées 'airflow' prête")

path = env["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"]
os.makedirs(os.path.dirname(path), exist_ok=True)
with open(path, "w") as f:
    json.dump({env["AIRFLOW_ADMIN_USER"]: env["AIRFLOW_ADMIN_PASSWORD"]}, f)
os.chmod(path, 0o644)
print("[airflow-init] compte de l'interface écrit")

sys.exit(subprocess.call(["airflow", "db", "migrate"]))
