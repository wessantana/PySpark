FROM bitnami/spark:3.5.0

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

CMD ["spark-submit", "src/data_collection.py"]