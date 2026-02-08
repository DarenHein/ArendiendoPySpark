from pyspark.sql import SparkSession

# 1. Iniciar la sesión
spark = SparkSession.builder \
    .appName("MiPrimeraVezConSpark") \
    .getOrCreate()

# 2. Crear unos datos de prueba (una lista de tuplas)
datos = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
columnas = ["Nombre", "Edad"]

# 3. Crear un DataFrame (como una tabla de Excel pero potente)
df = spark.createDataFrame(datos, schema=columnas)

# 4. Mostrar los datos
print("Mostrando el contenido del DataFrame:")
df.show()

# 5. Hacer una operación simple: Filtrar mayores de 30
print("Filtrando mayores de 30 años:")
df.filter(df.Edad > 30).show()

# 6. Cerrar la sesión
spark.stop()