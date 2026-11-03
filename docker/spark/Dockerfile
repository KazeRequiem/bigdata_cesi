# syntax=docker/dockerfile:1
# Spark 3.5 + Iceberg (catalogue JDBC dans PostgreSQL) + accès S3 + driver PostgreSQL
FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu

USER root

ARG ICEBERG_VERSION=1.6.1
ARG MAVEN=https://repo1.maven.org/maven2

# Jars téléchargés une fois dans l'image : pas de --packages au lancement
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/

RUN pip3 install --no-cache-dir "openpyxl>=3.1" "pytest>=8"

ENV PATH=/opt/spark/bin:$PATH \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

COPY silver /usr/local/bin/silver
RUN sed -i 's/\r$//' /usr/local/bin/silver && chmod 755 /usr/local/bin/silver \
 && mkdir -p /data/warehouse /data/bronze && chown -R spark:spark /data \
 && python3 -c "import pyspark, openpyxl; print('pyspark', pyspark.__version__)"

WORKDIR /opt/silver
USER spark
