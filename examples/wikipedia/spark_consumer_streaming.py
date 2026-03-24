from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, desc, when, sum
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "wikipedia_edits"

def run_spark_streaming():
    print("Iniciando sesión de Spark...")
    
    # 1. Inicializamos Spark Session
    # El config 'spark.jars.packages' es vital para descargar el conector de Kafka.
    # IMPORTANTE: Cambia '4.1.1' por tu versión exacta de Spark si es diferente.
    spark = SparkSession.builder \
        .appName("WikipediaStreamingAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .getOrCreate()
        
    # Reducimos el nivel de logs (INFO) de Spark para que no ensucie la consola
    spark.sparkContext.setLogLevel("WARN")
    
    # 2. Definimos el esquema del JSON que esperamos recibir.
    # Spark ignorará los campos del JSON original que no declaremos aquí.
    json_schema = StructType([
        StructField("user", StringType(), True),
        StructField("title", StringType(), True),
        StructField("wiki", StringType(), True),
        StructField("bot", BooleanType(), True)
    ])

    print(f"Conectando a Kafka en {KAFKA_BROKER}, leyendo el topic '{KAFKA_TOPIC}'...")

    # 3. Leemos el flujo de datos (Stream) desde Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Transformamos los datos crudos
    # Kafka nos da una columna 'value' en formato binario y una columna 'timestamp'.
    # Convertimos 'value' a String y luego parseamos el JSON usando nuestro esquema.
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_string", "timestamp") \
        .select(from_json(col("json_string"), json_schema).alias("data"), "timestamp") \
        .select("data.*", "timestamp")

    # 5. Agregación en tiempo real (Windowing)
    # Contamos las ediciones agrupando por ventanas de 20 segundos y si es bot o no.
    aggregated_df = parsed_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(window(col("timestamp"), "20 seconds")) \
        .agg(
            sum(when(col("bot") == True, 1).otherwise(0)).alias("bots"),
            sum(when(col("bot") == False, 1).otherwise(0)).alias("humans")
        ) \
        .orderBy(desc("window")) # Mantiene la ventana más reciente en la parte superior

    print("¡Procesamiento en tiempo real iniciado! Esperando el primer lote de datos...\n")

    # 6. Escribimos el resultado en la consola
    # Probad a cambiar el outputMode entre "complete", "update" y "append", son útiles en diferentes situaciones
    query = aggregated_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Mantenemos el proceso vivo hasta que se interrumpa manualmente
    query.awaitTermination()

if __name__ == "__main__":
    run_spark_streaming()