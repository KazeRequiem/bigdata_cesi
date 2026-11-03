# Superset + driver PostgreSQL (absent de l'image officielle)
FROM apache/superset:4.1.2
USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
COPY init.sh /app/init_chu.sh
RUN sed -i 's/\r$//' /app/init_chu.sh && chmod 755 /app/init_chu.sh
USER superset
