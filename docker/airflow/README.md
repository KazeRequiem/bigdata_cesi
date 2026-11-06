# Airflow : orchestration de la chaîne Silver -> Gold

Le Livrable 1 prévoit Airflow pour enchaîner les traitements, avec capteurs sur les manifestes Bronze,
reprises automatiques et alerte. Cette version est **minimale** : un DAG, `chu_pipeline`.

Interface : http://localhost:8080 (compte `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` du `.env`).

## Ce qui est en place

| Élément | Rôle |
|---|---|
| `airflow-init` | crée la base `airflow` (rôle dédié) dans le PostgreSQL du projet, écrit le compte de l'interface, migre la base, puis s'arrête |
| `airflow-apiserver` | interface web et API |
| `airflow-dag-processor` | lit `airflow/dags/` |
| `airflow-scheduler` | planifie et **exécute** les tâches (LocalExecutor) ; seul à monter le socket Docker |
| `airflow/dags/chu_pipeline.py` | le DAG |

## Le DAG `chu_pipeline`

```
attendre_s2_etablissements -> attendre_s0_cog -> attendre_s1_soins -> attendre_s1_hospitalisations
  -> attendre_s3_satisfaction -> attendre_s4_deces
  -> silver_ddl -> silver_communes -> silver_etablissements -> silver_professionnels -> silver_diagnostics
  -> silver_patients -> silver_consultations -> silver_hospitalisations -> silver_satisfaction -> silver_deces
  -> export_staging -> gold_dimensions (sql/08) -> gold_faits (sql/09) -> gold_copie_v0 (sql/10) -> rapport_qualite
```

- **Capteurs** : une tâche par source Bronze, qui attend au moins un `*.manifest.json` dans `bronze/<source>/<date>/`
  (lecture de l'API du filer SeaweedFS, sans identifiant). S2 (établissements) d'abord, comme dans le Livrable 1.
  Attente maximale 10 minutes par source, vérification toutes les 30 s.
- **Silver** : un job par table, dans l'ordre des dépendances (référentiels, entités, faits), donc une reprise
  ne rejoue que le job en échec. Une seule tâche à la fois (`max_active_tasks=1`) : Spark a 4 Go.
- **Gold** : export vers `staging`, puis les trois scripts SQL (rejouables), puis le rapport de qualité
  `silver/docs/rapport_qualite_airflow.md` (écrasé à chaque exécution).
- **Reprises** : 2 reprises par tâche, 2 minutes d'attente puis délai doublé.
- **Alerte** : après l'échec définitif d'une tâche, message dans son journal (`ALERTE ...`) et, si
  `ALERT_WEBHOOK_URL` est défini dans le `.env`, appel de ce webhook (Teams, Slack...).
- **Déclenchement** : manuel (bouton « Trigger »). Pour un lot quotidien, mettre `schedule="0 2 * * *"` dans le DAG.

## Premier lancement

1. Ajouter au `.env` les variables `AIRFLOW_*` de `.env.example` (clé Fernet : commande indiquée dans le fichier).
2. `docker compose up -d --build`
3. Ouvrir http://localhost:8080, activer le DAG `chu_pipeline` (interrupteur) puis « Trigger ».

Depuis un terminal :

```bash
docker compose exec airflow-scheduler airflow dags trigger chu_pipeline
docker compose exec airflow-scheduler airflow tasks test chu_pipeline silver_ddl 2026-01-01   # une tâche seule
```

## Comment les tâches agissent

Chaque tâche est un `docker exec` : `chu-spark` pour `silver ...`, `chu-postgres` pour `psql -f /sql/...`.
Conséquences voulues : **Airflow ne détient aucun secret du projet** (Silver lit les siens dans OpenBao, `psql`
passe par le socket local du conteneur PostgreSQL), et les mêmes commandes restent utilisables à la main.

## Limites assumées (à dire dans le rapport)

- **Le socket Docker donne à `airflow-scheduler` le contrôle de Docker sur la machine** (équivalent root).
  Acceptable pour une maquette locale ; en production : Spark soumis par `SparkSubmitOperator` ou
  `KubernetesPodOperator`, et PostgreSQL atteint par une connexion Airflow dont le mot de passe vient d'OpenBao.
- Le capteur teste la présence d'un manifeste, pas sa fraîcheur : il ne distingue pas le lot du jour d'un lot ancien.
- Pas de triggerer, de Celery ni de Keycloak (comptes locaux « SimpleAuthManager »).
- Niveau 2 prévu avec OpenBao : un compte AppRole pour Airflow et les connexions lues dans le coffre.
