from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DIM_MODE_PAIEMENT") \
    .config("spark.jars", "/opt/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

jdbc_staging = "jdbc:postgresql://postgres-staging:5432/flex_staging"
jdbc_dwh = "jdbc:postgresql://postgres-dwh:5432/flex_dwh"

properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

paiements = spark.read.jdbc(
    jdbc_staging,
    "staging.paiements",
    properties=properties
)

dim_mode = paiements.select(
    "mode_paiement",
    "type_caisse"
).distinct()

dim_mode = dim_mode.withColumn(
    "mode_paiement_key",
    monotonically_increasing_id()
)

dim_mode.write \
    .mode("overwrite") \
    .jdbc(
        jdbc_dwh,
        "dwh.dim_mode_paiement",
        properties=properties
    )

print("DIM_MODE_PAIEMENT créée")

spark.stop()