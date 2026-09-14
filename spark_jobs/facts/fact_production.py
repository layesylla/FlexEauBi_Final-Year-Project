from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit
)

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("FACT_PRODUCTION") \
    .config("spark.jars", "/opt/jars/postgresql-42.7.8.jar") \
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

production = spark.read.jdbc(
    jdbc_staging,
    "staging.production",
    properties=properties
)

# ==========================
# LECTURE DIMENSIONS
# ==========================

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
# CALCUL DES MESURES
# ==========================

production = production.withColumn(
    "volume_produit_m3",
    col("nouveau_index") - col("ancien_index")
)

production = production.withColumn(
    "nb_productions",
    lit(1)
)

production = production.withColumn(
    "compteur_anomalie",
    when(
        col("etat_compteur") == "ANOMALIE",
        1
    ).otherwise(0)
)

# ==========================
# JOINTURE DIM_DATE
# ==========================

p = production.alias("p")
d = dim_date.alias("d")
f = dim_forage.alias("f")
g = dim_geo.alias("g")

fact = p.join(
    d,
    p["date_traitement"] == d["date_complete"],
    "left"
)

fact = fact.join(
    f,
    p["id_forage"] == f["forage_id"],
    "left"
)

fact = fact.join(
    g,
    p["site_id"] == g["site_id"],
    "left"
)
# ==========================
# SELECTION FINALE
# ==========================
fact_production = fact.select(
    col("d.date_key").alias("date_key"),

    col("g.geo_key").alias("geo_key"),

    col("f.forage_key").alias("forage_key"),

    col("p.ancien_index"),

    col("p.nouveau_index"),

    col("p.volume_produit_m3"),

    col("p.nb_productions"),

    col("p.compteur_anomalie")
)

print("Nombre lignes :", fact_production.count())

fact_production.show(
    20,
    False
)

# ==========================
# CHARGEMENT DWH
# ==========================

fact_production.write \
    .mode("overwrite") \
    .jdbc(
        jdbc_dwh,
        "dwh.fact_production",
        properties=properties
    )

print("FACT_PRODUCTION créée avec succès")

spark.stop()