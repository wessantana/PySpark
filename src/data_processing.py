from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, sum
import os

spark = SparkSession.builder.appName("AppSpark").config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")


try:
    lista_csv = [f for f in os.listdir("data/raw/") if f.lower().endswith(".csv")]
    print(lista_csv)
    for csv in lista_csv:
        csv_lido = spark.read.csv(f"data/raw/{csv}", header=True, sep=";", encoding="ISO-8859-1", quote='"', escape='"')    
        #csv_lido.drop_duplicates().na.fill()
        csv_lido.show(10, truncate=False, vertical=True)
        #print(csv_lido.show(10, truncate=False, vertical=True))
finally:
    spark.stop()