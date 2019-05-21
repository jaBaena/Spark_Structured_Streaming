import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType


def main(directory) -> None:

    # Data link: https://datosabiertos.malaga.eu/recursos/aparcamientos/ocupappublicosmun/ocupappublicosmun.csv


    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .master("local[4]") \
        .appName("StreamingAverageOccupiedSlots") \
        .getOrCreate()

    # Set the StructType
    fields = (StructField("poiID", IntegerType(), True),
              StructField("nombre", StringType(), True),
              StructField("direccion", StringType(), True),
              StructField("telefono", StringType(), True),
              StructField("correoelectronico", StringType(), True),
              StructField("latitude", DoubleType(), True),
              StructField("longitude", DoubleType(), True),
              StructField("altitud", IntegerType(), True),
              StructField("capacidad", IntegerType(), True),
              StructField("capacidad_discapacitados", IntegerType(), True),
              StructField("fechahora_ultima_actualizacion", TimestampType(), True),
              StructField("libres", IntegerType(), True),
              StructField("libres_discapacitados", IntegerType(), True),
              StructField("nivelocupacion_naranja", IntegerType(), True),
              StructField("nivelocupacion_rojo", IntegerType(), True),
              StructField("smassa_sector_sare", StringType(), True))

    # Read the stream with that StructType (Data link: https://datosabiertos.malaga.eu/recursos/aparcamientos/ocupappublicosmun/ocupappublicosmun.csv)
    lines = spark \
        .readStream \
        .format("csv") \
        .options(header='false') \
        .schema(StructType(fields)) \
        .load(directory)

    # Add a new column of slots occupated
    parkings_df = lines.withColumn("ocupados", (lines.capacidad - lines.libres))

    # The query
    values = parkings_df.select("nombre", "capacidad", "libres", "ocupados")\
        .where("capacidad > 0") \
        .groupBy("nombre", "capacidad") \
        .agg(functions.mean("libres"), functions.mean("ocupados"))\
        .orderBy("nombre")


    values.printSchema()

    # Start running the query that prints the output in the screen
    query = values \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit StreamingAverageOccupiedSlots.py <directory>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
