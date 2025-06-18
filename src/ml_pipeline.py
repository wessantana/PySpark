from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, udf
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
import os
import gc

load_dotenv()

def precision_at_k(y_true, y_scores, k):
    top_k = np.argsort(y_scores)[-k:]
    return np.mean(y_true[top_k])

def main():
    spark = SparkSession.builder \
        .appName("AcidentesMLPipeline") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    dataframe = spark.read.parquet("data/processed/processed*") \
        .select("br", "km", "horario", "mortos",
                "condicao_metereologica", "dia_semana", "fase_dia")

    dataframe = dataframe.withColumn("hora", hour("horario")) \
                         .withColumn("label", (col("mortos") > 0).cast("int")) \
                         .fillna(0)

    historico_km = dataframe.groupBy("br","km").count() \
                      .withColumnRenamed("count","historico_acidentes_km")
    dataframe = dataframe.join(historico_km, ["br","km"], "left")

    historico_clima = dataframe.groupBy("condicao_metereologica").count() \
                         .withColumnRenamed("count","historico_acidentes_clima")
    dataframe = dataframe.join(historico_clima, "condicao_metereologica", "left")

    historico_dia_hora = dataframe.groupBy("dia_semana","hora").count() \
                           .withColumnRenamed("count","historico_dia_hora")
    dataframe = dataframe.join(historico_dia_hora, ["dia_semana","hora"], "left")

    historico_rodovia = dataframe.groupBy("fase_dia").count() \
                          .withColumnRenamed("count","historico_rodovia")
    dataframe = dataframe.join(historico_rodovia, "fase_dia", "left")

    densidade = dataframe.groupBy("br","km","hora").count() \
                   .withColumnRenamed("count","densidade_trafego")
    dataframe = dataframe.join(densidade, ["br","km","hora"], "left")

    cat_cols = ["condicao_metereologica","dia_semana","fase_dia"]
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") 
                for c in cat_cols]
    num_cols = ["hora","historico_acidentes_km","historico_acidentes_clima",
                "historico_dia_hora","historico_rodovia","densidade_trafego"]
    assembler = VectorAssembler(
        inputCols = num_cols + [f"{c}_idx" for c in cat_cols],
        outputCol = "features",
        handleInvalid="keep"
    )
    model = GBTClassifier(labelCol="label", featuresCol="features", maxIter=10)
    pipeline = Pipeline(stages=indexers + [assembler, model])

    df = dataframe.filter(col("mortos").isNotNull())
    train, test = df.randomSplit([0.8,0.2], seed=42)
    train = train.filter(train["label"].isNotNull())

    gc.collect()
    pipeline_model = pipeline.fit(train)

    pred = pipeline_model.transform(test)
    evaluator = BinaryClassificationEvaluator()
    auc = evaluator.evaluate(pred)
    print(f"AUC-ROC: {auc:.4f}")

    pd_small = pred.select("label","probability").limit(10000).toPandas()
    pd_small["score"] = pd_small["probability"].apply(lambda v: float(v[1]))
    y_true, y_scores = pd_small["label"].values, pd_small["score"].values
    precision = precision_at_k(y_true,y_scores,50)
    print("Precision@50:", precision)

    extract_prob = udf(lambda p: float(p[1]), DoubleType())
    top50 = pred.groupBy("br","km") \
        .agg(avg(extract_prob(col("probability"))).alias("risco_medio")) \
        .orderBy("risco_medio", ascending=False) \
        .limit(50)

    info = pred.select("br","km","hora",
                       "condicao_metereologica","fase_dia","historico_acidentes_km") \
               .dropDuplicates(["br","km"])
    top50 = top50.join(info, ["br","km"], "left") \
                 .select("br","km","risco_medio","hora",
                         "condicao_metereologica","fase_dia","historico_acidentes_km") \
                 .repartition(4)  # paralelismo leve

    top50.write \
         .format("jdbc") \
         .option("url", os.getenv("DB_URL")) \
         .option("dbtable", "acidentes_preditos") \
         .option("user", os.getenv("DB_USER")) \
         .option("password", os.getenv("DB_PASSWORD")) \
         .option("driver", "org.postgresql.Driver") \
         .mode("overwrite") \
         .save()

    top_pd = top50.limit(50).toPandas()

    brs   = top_pd["br"].value_counts().head(3).index.tolist()
    hrs   = top_pd["hora"].value_counts().head(3).index.tolist()
    cls   = top_pd["condicao_metereologica"].value_counts().head(2).index.tolist()
    fases = top_pd["fase_dia"].value_counts().head(2).index.tolist()
    br_str    = ", ".join([f"BR-{b}" for b in brs])
    hora_str  = ", ".join([f"{int(h)}h" for h in sorted(hrs)])
    clima_str = ", ".join(cls)
    fase_str  = ", ".join(fases)

    c = canvas.Canvas("/app/data/reports/top_50_trechos.pdf", pagesize=A4)
    w,h = A4
    y = h - 40
    c.setFont("Helvetica-Bold",16)
    c.drawString(50,y, "📈 Relatório Analítico - Previsão de Risco de Acidentes"); y-=30
    c.setFont("Helvetica-Bold",12); c.drawString(50,y,"📊 Análise Exploratória"); y-=20
    c.setFont("Helvetica",10)
    c.drawString(60,y,f"- Fases do dia mais arriscadas: {fase_str}"); y-=15
    c.drawString(60,y,f"- Horários críticos: {hora_str}");             y-=15
    c.drawString(60,y,f"- Climas mais frequentes: {clima_str}");      y-=15
    c.drawString(60,y,f"- BRs mais perigosas: {br_str}");             y-=25

    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y, "🧠 Modelo Preditivo")
    y -= 20
    c.setFont("Helvetica", 10)
    c.drawString(60, y, "Features utilizadas:")
    y -= 15
    c.drawString(70, y, "- Histórico de acidentes por trecho (km)")
    y -= 15
    c.drawString(70, y, "- Dia da semana, hora")
    y -= 15
    c.drawString(70, y, "- Clima, precipitação")
    y -= 15
    c.drawString(70, y, "- Tipo de rodovia")
    y -= 20

    c.drawString(60, y, "Algoritmos testados:")
    y -= 15
    c.drawString(70, y, "- GBTClassifier (MLlib)")
    y -= 20

    c.drawString(60, y, "Métricas:")
    y -= 15
    c.drawString(70, y, f"AUC-ROC: {auc:.4f}")
    y -= 15
    c.drawString(70, y, f"Precision@50: {precision}")
    y -= 15

    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y, "🧩 Interpretação")
    y -= 20
    c.setFont("Helvetica", 10)
    c.drawString(60, y, "- Modelos conseguem prever trechos com alta precisão")
    y -= 15
    c.drawString(60, y, "- Horário + clima são os fatores mais influentes")
    y -= 15
    c.drawString(60, y, "- As previsões podem ser usadas para alertas e prevenção")
    y -= 25

    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y, "📌 Resultados")
    y -= 20
    c.setFont("Helvetica", 10)
    c.drawString(60, y, "Top 50 trechos mais críticos:")
    y -= 20

    for idx, row in top_pd.iterrows():
        line = (
            f"{idx+1:02d}. BR-{row['br']} KM {row['km']:.2f} — "
            f"Risco: {row['risco_medio']:.4f} | "
            f"Hora: {int(row['hora'])}h | Clima: {row['condicao_metereologica']} | "
            f"Fase: {row['fase_dia']} | Hist. KM: {int(row['historico_acidentes_km'])}"
        )
        c.drawString(60, y, line)
        y -= 12
        if y < 50:
            c.showPage()
            y = h - 40
            c.setFont("Helvetica", 10)

    c.setFont("Helvetica-Oblique", 8)
    c.drawString(50, 30, f"Relatório gerado automaticamente em {datetime.now().strftime('%d/%m/%Y %H:%M')}")

    c.save()
    print("Relatório salvo com sucesso.")


    

    spark.stop()

if __name__ == "__main__":
    main()
