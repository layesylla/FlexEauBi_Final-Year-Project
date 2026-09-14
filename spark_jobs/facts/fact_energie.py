from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("FACT_ENERGIE") \
    .config("spark.jars", "/opt/jars/postgresql-42.7.8.jar") \
    .getOrCreate()

jdbc_staging = "jdbc:postgresql://postgres-staging:5432/flex_staging"
jdbc_dwh = "jdbc:postgresql://postgres-dwh:5432/flex_dwh"

properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# ==========================
# Sources
# ==========================

energie = spark.read.jdbc(
    jdbc_staging,
    "staging.consommation_energetique",
    properties=properties
)

dim_date = spark.read.jdbc(
    jdbc_dwh,
    "dwh.dim_date",
    properties=properties
)

dim_geo = spark.read.jdbc(
    jdbc_dwh,
    "dwh.dim_geographie",
    properties=properties
)

dim_forage = spark.read.jdbc(
    jdbc_dwh,
    "dwh.dim_forage",
    properties=properties
)

# ==========================
# Mesures
# ==========================

energie = energie.withColumn(
    "kwh_consomme",
    col("nouveau_index") - col("ancien_index")
)

energie = energie.withColumn(
    "cout_energie_fcfa",
    col("cout_fcfa")
)

energie = energie.withColumn(
    "nb_consommations",
    lit(1)
)

# ==========================
# Jointures
# ==========================

e = energie.alias("e")
d = dim_date.alias("d")
f = dim_forage.alias("f")
g = dim_geo.alias("g")

fact = e.join(
    d,
    e["date_traitement"] == d["date_complete"],
    "left"
)

fact = fact.join(
    f,
    e["id_forage"] == f["forage_id"],
    "left"
)

fact = fact.join(
    g,
    f["site_id"] == g["site_id"],
    "left"
)

# ==========================
# Sélection finale
# ==========================

fact_energie = fact.select(
    col("d.date_key").alias("date_key"),

    col("g.geo_key").alias("geo_key"),

    col("f.forage_key").alias("forage_key"),

    col("e.kwh_consomme"),

    col("e.cout_energie_fcfa"),

    col("e.nb_consommations")
)

print("Nombre lignes :", fact_energie.count())

fact_energie.show(20, False)

# ==========================
# Chargement DWH
# ==========================

fact_energie.write \
    .mode("overwrite") \
    .jdbc(
        jdbc_dwh,
        "dwh.fact_energie",
        properties=properties
    )

print("FACT_ENERGIE créée avec succès")

spark.stop()