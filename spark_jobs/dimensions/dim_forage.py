from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DIM_FORAGE") \
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

forages = spark.read.jdbc(
    jdbc_staging,
    "staging.forages",
    properties=properties
)

sources = spark.read.jdbc(
    jdbc_staging,
    "staging.sources_energie",
    properties=properties
)

# ==========================
# CONSTRUCTION DIMENSION
# ==========================

dim_forage = (
    forages.alias("f")
    .join(
        sources.alias("s"),
        forages["source_energie_id"] == sources["id"],
        "left"
    )
    .select(
        forages["id"].alias("forage_id"),
        forages["code_forage"],
        forages["nom_forage"],

        forages["site_id"],
        forages["commune_id"],

        forages["source_energie_id"],

        sources["energie"].alias("source_energie"),

        forages["arrete"],
        forages["transfere"]
    )
)

# ==========================
# CLE SUBSTITUT
# ==========================

dim_forage = dim_forage.withColumn(
    "forage_key",
    monotonically_increasing_id()
)

dim_forage = dim_forage.select(
    "forage_key",

    "forage_id",
    "code_forage",
    "nom_forage",

    "site_id",
    "commune_id",

    "source_energie_id",
    "source_energie",

    "arrete",
    "transfere"
)

print("Nombre lignes :", dim_forage.count())

dim_forage.show(20, False)

# ==========================
# CHARGEMENT DWH
# ==========================

dim_forage.write \
    .mode("overwrite") \
    .jdbc(
        jdbc_dwh,
        "dwh.dim_forage",
        properties=properties
    )

print("DIM_FORAGE créée avec succès")

spark.stop()