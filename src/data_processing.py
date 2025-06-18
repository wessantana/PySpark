from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, regexp_replace, to_date, to_timestamp, lit
from pyspark.sql.types import IntegerType, DoubleType
import os

spark = SparkSession.builder.appName("AppSpark").config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.memory.offHeap.enabled", "true",) \
    .config("spark.memory.offHeap.size", "2g") \
    .getOrCreate()

    
spark.sparkContext.setLogLevel("WARN")

def transformar_csv_em_dataframe(caminho: str):
    try:
        lista_csv = [f for f in os.listdir(caminho) if f.lower().endswith(".csv")]
        lista_dataframes = []

        for nome_csv in lista_csv:
            dataframe = spark.read.csv(f"{caminho}{nome_csv}", header=True, sep=";", encoding="ISO-8859-1", quote='"', escape='"')
            lista_dataframes.append(dataframe)
    except Exception as e:
        print(f"Não foi possível converter o(s) CSV(s) em dataframe: {e}")

    print("O(s) CSV(s) foram convertidos com sucesso!")

    return lista_dataframes

def padronizar_dataframe():
    colunas_numericas = ["ilesos", "feridos_leves", "feridos_graves", "mortos", "idade"]
    colunas_texto = [
        "dia_semana", "uf", "br", "municipio", "causa_principal", "causa_acidente",
        "ordem_tipo_acidente", "tipo_acidente", "classificacao_acidente", "fase_dia",
        "sentido_via", "condicao_metereologica", "tipo_pista", "tracado_via",
        "uso_solo", "tipo_veiculo", "marca", "tipo_envolvido", "estado_fisico",
        "sexo", "regional", "delegacia", "uop"
    ]
    
    lista_dataframes = transformar_csv_em_dataframe(caminho="data/raw/")
    dataframes_padronizados = []

    try:
        for dataframe in lista_dataframes:
            dataframe = dataframe.dropDuplicates()

            dataframe = dataframe.dropna(subset=["id", "data_inversa"])

            dataframe = dataframe.select([
                trim(regexp_replace(col(c), r"[\n\r\t]", "")).alias(c) for c in dataframe.columns
            ])

            dataframe = dataframe.withColumn("data_inversa", to_date(col("data_inversa"), "yyyy-MM-dd"))
            dataframe = dataframe.withColumn("horario", to_timestamp(col("horario"), "HH:mm:ss"))

            dataframe = dataframe.select([
                when(col(c).isin("NA", "NA/NA", "Não Informado", "", "nan", "null", "None"), None)
                .otherwise(col(c)).alias(c)
                for c in dataframe.columns
            ])

            dataframe = dataframe.filter(col("latitude").isNotNull() & col("longitude").isNotNull())

            dataframe = dataframe.dropna(subset=["br", "km", "data_inversa"])

            dataframe = dataframe.withColumn("km", regexp_replace(col("km"), ",", ".").cast(DoubleType()))
            dataframe = dataframe.withColumn("latitude", regexp_replace(col("latitude"), ",", ".").cast(DoubleType()))
            dataframe = dataframe.withColumn("longitude", regexp_replace(col("longitude"), ",", ".").cast(DoubleType()))

            for coluna in colunas_numericas:
                dataframe = dataframe.withColumn(coluna, col(coluna).cast(IntegerType()))

            for coluna in colunas_texto:
                dataframe = dataframe.withColumn(
                    coluna, when(col(coluna).isNull(), lit("unknown")).otherwise(col(coluna))
                )

            dataframes_padronizados.append(dataframe)
    except Exception as e:
        print(f"Não foi possível padronizar o(s) dataframe(s): {e}")
    return dataframes_padronizados




def salvar_dataframe(dataframes:list, caminho_para_salvar:str="data/processed/"):

    try:
        lista_dataframes = dataframes
        index_nome_arquivo = 1
        for dataframe in lista_dataframes:
            dataframe.repartition(10).write.mode("overwrite").option("header", True).parquet(f"{caminho_para_salvar}processed{index_nome_arquivo}")
            index_nome_arquivo += 1
    except Exception as e:
        print(f"Não foi possível salvar o dataframe: {e}")


try:

    dataframes_padronizados = padronizar_dataframe()
    salvar_dataframe(dataframes_padronizados, "data/processed/")

except Exception as e:
    print(f"Não foi possível processar os dados: {e}")
    
finally:
    spark.stop()

