from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, sum, trim, when, regexp_replace, to_date, to_timestamp
from pyspark.sql.types import IntegerType, DoubleType
import os

spark = SparkSession.builder.appName("AppSpark").config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.memory.offHeap.enabled", "true",) \
    .config("spark.memory.offHeap.size", "2g") \
    .getOrCreate()

    
spark.sparkContext.setLogLevel("ERROR")

def transformar_csv_em_dataframe(caminho: str):

    lista_csv = [f for f in os.listdir(caminho) if f.lower().endswith(".csv")]
    lista_dataframes = []

    for nome_csv in lista_csv:
        dataframe = spark.read.csv(f"{caminho}{nome_csv}", header=True, sep=";", encoding="ISO-8859-1", quote='"', escape='"')
        lista_dataframes.append(dataframe)
    return lista_dataframes

def padronizar_dataframe():

    colunas_numericas = ["ilesos", "feridos_leves", "feridos_graves", "mortos", "idade"]
    lista_dataframes = transformar_csv_em_dataframe(caminho="data/raw/")
    dataframes_padronizados = []


    for dataframe in lista_dataframes:

        dataframe = dataframe.dropDuplicates()

        dataframe = dataframe.dropna(subset=["id", "data_inversa"])

        dataframe = dataframe.select([trim(col(c)).alias(c) for c in dataframe.columns])

        dataframe = dataframe.filter(col("latitude").isNotNull() & col("longitude").isNotNull())

        dataframe = dataframe.withColumn("data_inversa", to_date(col("data_inversa"), "yyyy-MM-dd"))

        dataframe = dataframe.withColumn("horario", to_timestamp(col("horario"), "HH:mm:ss"))

        dataframe = dataframe.select([when(col(c).isin("NA", "NA/NA", "Não Informado", ""), None).otherwise(col(c)).alias(c) for c in dataframe.columns])
    
        dataframe = dataframe.withColumn("km", regexp_replace(col("km"), ",", ".").cast(DoubleType()))

        dataframe = dataframe.withColumn("latitude", regexp_replace(col("latitude"), ",", ".").cast(DoubleType()))
        
        dataframe = dataframe.withColumn("longitude", regexp_replace(col("longitude"), ",", ".").cast(DoubleType()))

        for coluna in colunas_numericas:
            dataframe = dataframe.withColumn(coluna, col(coluna).cast(IntegerType()))

        dataframes_padronizados.append(dataframe)

    return dataframes_padronizados




def salvar_dataframe(dataframes:list, caminho_para_salvar:str="data/processed/"):
    
    # O problema de OutOffMemory foi solucionado aumentando a memória, mas não é uma solução definitiva.

    lista_dataframes = dataframes
    index_nome_arquivo = 1
    for dataframe in lista_dataframes:
        dataframe.write.mode("overwrite").option("header", True).parquet(f"{caminho_para_salvar}processed{index_nome_arquivo}")
        index_nome_arquivo += 1


try:

    dataframes_padronizados = padronizar_dataframe()
    salvar_dataframe(dataframes_padronizados, "data/processed/")

except Exception as e:
    print(f"Não foi possível concluir a operação: {e}")
finally:
    spark.stop()

