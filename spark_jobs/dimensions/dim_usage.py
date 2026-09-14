from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DIM_USAGE") \
    .config(
        "spark.jars",
        "/opt/jars/postgresql-42.7.8.jar"
    ) \
    .getOrCreate()

# ==========================
# CONNEXIONS
# ==========================

jdbc_staging = "jdbc:postgresql://postgres-staging:5432/flex_staging"

jdbc_dwh = "jdbc:postgresql://postgres-dwh:5432/flex_dwh"

properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# ==========================
# LECTURE STAGING
# ==========================

usages = spark.read.jdbc(
    jdbc_staging,
    "staging.usages",
    properties=properties
)

# ==========================
# CONSTRUCTION DIMENSION
# ==========================

dim_usage = usages.select(
    usages["id"].alias("usage_id"),
    usages["code"].alias("code_usage"),
    usages["type_usage"],
    usages["tarif_ht"],
    usages["tva"],
    usages["surtaxe"]
)

# ==========================
# CLE SUBSTITUT
# ==========================

dim_usage = dim_usage.withColumn(
    "usage_key",
    monotonically_increasing_id()
)

dim_usage = dim_usage.select(
    "usage_key",
    "usage_id",
    "code_usage",
    "type_usage",
    "tarif_ht",
    "tva",
    "surtaxe"
)

print("Nombre lignes :", dim_usage.count())

dim_usage.show(20, False)

# ==========================
# CHARGEMENT DWH
# ==========================

dim_usage.write \
    .mode("overwrite") \
    .jdbc(
        jdbc_dwh,
        "dwh.dim_usage",
        properties=properties
    )

print("DIM_USAGE créée avec succès")

spark.stop()