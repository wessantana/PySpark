FROM bitnami/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    postgresql-client \
    libpq-dev \
    gcc \
    python3-dev \
    netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# 2. Variáveis de ambiente
ENV POSTGRES_JDBC_VERSION=42.5.4
ENV SPARK_EXTRA_JAR_PATH=/opt/bitnami/spark/jars
ENV PYTHONPATH=/opt/bitnami/spark/python:$PYTHONPATH

# 3. Agora o curl está disponível para baixar o JDBC
RUN curl -o ${SPARK_EXTRA_JAR_PATH}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar

WORKDIR /app
COPY requirements.txt .
COPY . .

# 4. Instalação de dependências Python
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

RUN chmod +x /app/src/*.py