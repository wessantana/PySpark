# ⚙️ Manual Técnico - Pipeline de Previsão de Acidentes

## 📌 Objetivo

Este pipeline processa dados públicos de acidentes rodoviários, realiza a engenharia de features e treina um modelo de machine learning para prever risco de acidentes por trecho, horário e condições climáticas.

---

## 📂 Estrutura do Projeto

src/
├── data_collection.py # Coleta e leitura de dados brutos
├── data_processing.py # Limpeza e padronização dos dados
├── ml_pipeline.py # Criação de variávieis para ML, treinamento e validação do modelo
├── postgres_insert.py # Inserção dos dados no PostgreSQL
└── dashboard.py # Interface com Streamlit


---

## 🔄 Pipeline ETL

### 1. `data_collection.py`
- Lê os dados da PRF (acidentes.csv)
- Pode integrar com APIs no futuro

### 2. `data_processing.py`
- Corrige tipos e formatações
- Remove valores inválidos ou nulos com `when(...).otherwise(...)`
- Converte campos para `DateType` / `DoubleType` no PySpark


### 3. `ml_pipeline.py`
- Gera variáveis como:
  - `hist_acidentes_km`
  - `semana_dia`, `hora`
- Junta com os dados originais por `br`, `km`
- Usa o algoritmo `GBTClassifier` (MLlib/Spark)
- Realiza pré-processamento com Pipeline (indexação, vetorização)
- Treina o modelo com 80% dos dados
- Avalia com AUC-ROC e Precision@K
- Agrupa os 50 trechos mais críticos por risco médio previsto
- Salva as previsões no PostgreSQL e gera um PDF com o relatório

---

## 🧪 Banco de Dados

- PostgreSQL
    - Usado para armazenar os resultados e previsões
        - Tabelas envolvidas:
        - `acidentes_processados_*`
        - `acidentes_preditos`
        - `trechos_criticos`
    - Usado para armazenar usuários e códigos de autenticação
        - `mfa_codes`,
        - `users`
---

## 🧰 Tecnologias

- Spark 3.5.0 e Streamlit (via Docker)
- Pandas, NumPy, Scikit-learn, Seaborn, Matplotlib, Requests, Reportlib
- PostgreSQL 15.13
- Docker Compose para orquestração local
