from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
# 1. Iniciar la sesión
spark = SparkSession.builder \
    .appName("MiPrimeraVezConSpark") \
    .getOrCreate()

df = spark.read.option("header", "true").csv("pokemon.csv")

def FiltradoPorTipo(df):
    df.filter(col("Type1") == "Fire")
    return df 

dfFiltrado = FiltradoPorTipo(df)



# todo resultados 
dfFiltrado.count()
dfFiltrado.show(5)

spark.stop