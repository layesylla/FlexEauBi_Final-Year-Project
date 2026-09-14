from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DIM_ABONNE") \
    .config("spark.jars", "/opt/jars/postgresql-42.7.8.jar") \
    .getOrCreate()


# CONNEXIONS

jdbc_staging = "jdbc:postgresql://postgres-staging:5432/flex_staging"

jdbc_dwh = "jdbc:postgresql://postgres-dwh:5432/flex_dwh"

properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


# LECTURE STAGING


abonnes = spark.read.jdbc(
    jdbc_staging,
    "staging.abonnes",
    properties=properties
)

# ==========================
# CONSTRUCTION DIMENSION
# ==========================

dim_abonne = abonnes.select(
    abonnes["id"].alias("abonne_id"),
    abonnes["num_abonne"],
    abonnes["usager_id"],
    abonnes["usage_id"],
    abonnes["date_abonnement"],
    abonnes["statut_abonne"],
    abonnes["solde_courant"],
    abonnes["vol_moy_m3"]
)

# ==========================
# CLE SUBSTITUT
# ==========================

dim_abonne = dim_abonne.withColumn(
    "abonne_key",
    monotonically_increasing_id()
)

dim_abonne = dim_abonne.select(
    "abonne_key",

    "abonne_id",
    "num_abonne",

    "usager_id",
    "usage_id",

    "date_abonnement",

    "statut_abonne",

    "solde_courant",
    "vol_moy_m3"
)

print("Nombre lignes :", dim_abonne.count())

dim_abonne.show(20, False)

# ==========================
# CHARGEMENT DWH
# ==========================

dim_abonne.write \
    .mode("overwrite") \
    .jdbc(
        jdbc_dwh,
        "dwh.dim_abonne",
        properties=properties
    )

print("DIM_ABONNE créée avec succès")

spark.stop()