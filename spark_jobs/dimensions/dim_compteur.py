from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DIM_COMPTEUR") \
    .config("spark.jars", "/opt/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

jdbc_staging = "jdbc:postgresql://postgres-staging:5432/flex_staging"
jdbc_dwh = "jdbc:postgresql://postgres-dwh:5432/flex_dwh"

properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

compteurs = spark.read.jdbc(
    jdbc_staging,
    "staging.compteurs",
    properties=properties
)

dim_compteur = compteurs.select(
    compteurs["id"].alias("compteur_id"),
    compteurs["num_compteur"],
    compteurs["abonne_id"],
    compteurs["etat"]
)

dim_compteur = dim_compteur.withColumn(
    "compteur_key",
    monotonically_increasing_id()
)

dim_compteur.write \
    .mode("overwrite") \
    .jdbc(
        jdbc_dwh,
        "dwh.dim_compteur",
        properties=properties
    )

print("DIM_COMPTEUR créée")

spark.stop()