# PySpark - Previsão de Risco de Acidentes

Projeto de pipeline de dados com PySpark, Machine Learning (MLlib), e visualização interativa com Streamlit, focado na previsão de trechos rodoviários com maior risco de acidentes.

## 🧰 Tecnologias

* **Apache Spark (PySpark)**
* **scikit-learn** (para análises adicionais)
* **MLlib (GBTClassifier)**
* **PostgreSQL**
* **Streamlit** (interface interativa via Docker)
* **Docker & Docker Compose** (orquestração local)
* **ReportLab** (geração de PDFs)

## 🚀 Como Executar

1. Clone o repositório:

   ```bash
   git clone https://github.com/wessantana/PySpark.git
   cd PySpark
   ```

2. Crie o arquivo `.env` na raiz do projeto com as seguintes credenciais

   ```env
   DB_USER=dbuser
   DB_PASSWORD=password
   DB_URL=url
   DB_HOST=host
   DB_PORT=5432
   DB_NAME=dbname
   SMTP_SERVER=smtp.gmail.com
   SMTP_PORT=465
   SMTP_USER=user
   SMTP_PASS=pass
   ```

3. Levante os containers com Docker Compose:

   ```bash
   docker compose up --build
   ```

   Isso iniciará:

   * Spark (driver e executor)
   * PostgreSQL
   * Streamlit (interface em [http://localhost:8501](http://localhost:8501))

4. Acesse a interface:

   ```bash
   # No navegador
   http://localhost:8501
   ```

## 📂 Organização do Projeto

```
├── README.md                  # Documento de entrada (este arquivo)
├── docs/                      # Documentação técnica completa
│   ├── manual_tecnico.md      # Descrição do pipeline ETL e ML
│   ├── relatorio_analitico.md # Relatório de resultados e métricas
├── data/                      # Dados brutos e processados
│   ├── raw/                   # CSVs originais
│   └── processed/             # Parquets transformados
|   └── reports/               # Arquivo PDF gerado da análise
├── src/                       # Código-fonte do pipeline
│   ├── data_collection.py     # Coleta de dados brutos
│   ├── data_processing.py     # Limpeza e padronização
│   ├── ml_pipeline.py         # Criação de features, treinamento, predição e relatório
│   └── dashboard.py           # Dashboard Streamlit
├── tests/                     # Testes automatizados (pytest)
├── notebooks/                 # Notebooks de EDA
├── requirements.txt           # Dependências container Spark
├── requirements.txt           # Dependências container Streamlit
├── docker-compose.yml         # Orquestração Docker
├── Dockerfile                 # Orquestração container Spark
└── .env                       # Variáveis de ambiente
```

## 📈 Funcionalidades

* **Pipeline ETL completo**: coleta, limpeza, feature engineering e treinamento de modelo.
* **Modelo preditivo**: GBTClassifier do MLlib, com métricas AUC-ROC e Precision\@50.
* **Geração de relatórios PDF**: top 50 trechos críticos, seção Analítico e Interpretativo.
* **Interface interativa**: filtros por município, ano, condição e visualização de mapas e tabelas.

## 🎛️ Configuração do `docker-compose.yml`

```yaml
services:
  spark:
    build: .
    container_name: spark-app
    environment:
      - DB_URL=${DB_URL}
      - DB_USER=${DB_USER}
      - DB_PASSWORD=${DB_PASSWORD}
    volumes:
      - ./data:/app/data
      - ./src:/app/src
      - ./jars/postgresql-42.5.4.jar:/opt/bitnami/spark/jars/postgresql-42.5.4.jar
    ports:
      - "4040:4040"
    command: >
      bash -c "
      echo 'Iniciando pipeline Spark...';
      spark-submit --driver-class-path /opt/bitnami/spark/jars/postgresql-42.5.4.jar
      --jars /opt/bitnami/spark/jars/postgresql-42.5.4.jar
      /app/src/data_collection.py &&
      spark-submit --driver-class-path /opt/bitnami/spark/jars/postgresql-42.5.4.jar
      --jars /opt/bitnami/spark/jars/postgresql-42.5.4.jar
      /app/src/data_processing.py &&
      spark-submit --driver-class-path /opt/bitnami/spark/jars/postgresql-42.5.4.jar
      --jars /opt/bitnami/spark/jars/postgresql-42.5.4.jar
      /app/src/ml_pipeline.py
      "
  dashboard:
    image: python:3.11
    container_name: streamlit-app
    environment:
    - DB_URL=${DB_URL}
    - DB_USER=${DB_USER}
    - DB_PASSWORD=${DB_PASSWORD}
    depends_on:
    - spark
    working_dir: /app
    volumes:
    - ./src:/app/src
    - ./data:/app/data
    - ./dashboard-requirements.txt:/app/requirements.txt:ro
    - ./.env:/app/.env:ro
    ports:
    - "8501:8501"
    command: >
      bash -c "
      apt-get update && apt-get install -y libpq-dev &&
      pip install --upgrade pip &&
      pip install --no-cache-dir -r /app/requirements.txt &&
      streamlit run /app/src/dashboard.py --server.port=8501 --server.headless=true
      "

```

## 🔧 Testes

```bash
pytest tests/
```

## 🛠️ Futuras Melhorias

* Adicionar validação cruzada (CrossValidator) no pipeline ML.
* Integrar XGBoost para comparação de performance.
* Hospedar a interface no Streamlit Cloud ou outro serviço PaaS.

---
