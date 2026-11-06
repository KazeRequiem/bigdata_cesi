# Airflow 3 + client Docker : les tâches lancent Silver dans le conteneur chu-spark et les scripts SQL dans chu-postgres
FROM apache/airflow:3.3.2
USER root
COPY --from=docker:27-cli /usr/local/bin/docker /usr/local/bin/docker
USER airflow
RUN mkdir -p /opt/airflow/state   # dossier du volume airflow_state, propriété de l'utilisateur airflow
