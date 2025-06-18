from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, udf
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
import numpy as np
from dotenv import load_dotenv
import os

def precision_at_k(y_true, y_scores, k):
    top_k = np.argsort(y_scores)[-k:]
    return np.mean(y_true[top_k])

def main():

    spark = SparkSession.builder.appName("AcidentesMLPipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    dataframe = spark.read.parquet("data/processed/processed*")

    dataframe = dataframe.withColumn("hora", hour("horario"))

    historico_km = dataframe.groupBy("br", "km").count().withColumnRenamed("count", "historico_acidentes_km")
    dataframe = dataframe.join(historico_km, on=["br", "km"], how="left")

    historico_clima = dataframe.groupBy("condicao_metereologica").count().withColumnRenamed("count", "historico_acidentes_clima")
    dataframe = dataframe.join(historico_clima, on="condicao_metereologica", how="left")

    historico_dia_hora = dataframe.groupBy("dia_semana", "hora").count().withColumnRenamed("count", "historico_dia_hora")
    dataframe = dataframe.join(historico_dia_hora, on=["dia_semana", "hora"], how="left")

    historico_tipo_rodovia = dataframe.groupBy("tipo_pista").count().withColumnRenamed("count", "historico_rodovia")
    dataframe = dataframe.join(historico_tipo_rodovia, on="tipo_pista", how="left")

    densidade = dataframe.groupBy("br", "km", "hora").count().withColumnRenamed("count", "densidade_trafego")
    dataframe = dataframe.join(densidade, on=["br", "km", "hora"], how="left")

    dataframe = dataframe.withColumn("label", (col("mortos") > 0).cast("int"))

    dataframe = dataframe.fillna(0)

    colunas_categoricas = ["condicao_metereologica", "dia_semana", "tipo_pista", "fase_dia"]
    indexers = [StringIndexer(inputCol=coluna, outputCol=coluna+"_idx", handleInvalid="keep") for coluna in colunas_categoricas]
    encoders = OneHotEncoder(
        inputCols=[c+"_idx" for c in colunas_categoricas],
        outputCols=[c+"_vec" for c in colunas_categoricas]
    )

    colunas_numericas = ["hora", "historico_acidentes_km", "historico_acidentes_clima", "historico_dia_hora", "historico_rodovia", "densidade_trafego"]
    assembler = VectorAssembler(
        inputCols = colunas_numericas + [c+"_vec" for c in colunas_categoricas],
        outputCol = "features",
        handleInvalid="keep"
    )

    model = GBTClassifier(labelCol="label", featuresCol="features", maxIter=30)
    pipeline = Pipeline(stages=indexers + [encoders, assembler, model])

    dataframe = dataframe.filter(col("mortos").isNotNull())
    train, test = dataframe.randomSplit([0.8, 0.2], seed=42)
    train = train.filter(train["label"].isNotNull())

    pipeline_model = pipeline.fit(train)

    pred = pipeline_model.transform(test)

    evaluator = BinaryClassificationEvaluator()
    auc = evaluator.evaluate(pred)
    print(f"AUC-ROC: {auc:.4f}")

    pandas_pred = pred.select("label", "probability").toPandas()
    pandas_pred["score"] = pandas_pred["probability"].apply(lambda v: float(v[1]))
    y_true = pandas_pred["label"].values
    y_scores = pandas_pred["score"].values
    print("Precision@50:", precision_at_k(y_true, y_scores, 50))

    extract_prob = udf(lambda prob: float(prob[1]), DoubleType())
    

    top50_dataframe = pred.groupBy("br", "km") \
       .agg(avg(extract_prob(col("probability"))).alias("risco_medio")) \
       .orderBy("risco_medio", ascending=False) \
        .limit(50)
    
    top50_dataframe.write \
    .format("jdbc") \
    .option("url", os.getenv("DB_URL")) \
    .option("dbtable", "acidentes_preditos") \
    .option("user", os.getenv("DB_USER")) \
    .option("password", os.getenv("DB_PASSWORD")) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
    
    top50_pandas = top50_dataframe.toPandas()

    pdf_path = "/app/data/reports/top_50_trechos.pdf"
    c = canvas.Canvas(pdf_path, pagesize=A4)
    width, height = A4
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, height - 50, "Top 50 Trechos com o Maior Risco de Acidente")
    c.setFont("Helvetica", 10)
    y = height - 80

    for idx, row in top50_pandas.iterrows():
        line = f"{idx+1:02d}. BR-{row['br']} KM {row['km']:.2f} — Risco Médio: {row['risco_medio']:.4f}"
        c.drawString(50, y, line)
        y -= 15
        if y < 50:
            c.showPage()
            y = height - 50
            c.setFont("Helvetica", 10)

    c.save()

    print("Relatório salvo com sucesso.")

    

    spark.stop()

if __name__ == "__main__":
    main()
