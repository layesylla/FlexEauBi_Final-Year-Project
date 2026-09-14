from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit
)

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("FACT_FACTURATION") \
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
# SOURCES
# ==========================

factures = spark.read.jdbc(
    jdbc_staging,
    "staging.factures",
    properties=properties
)

dim_date = spark.read.jdbc(
    jdbc_dwh,
    "dwh.dim_date",
    properties=properties
)

dim_abonne = spark.read.jdbc(
    jdbc_dwh,
    "dwh.dim_abonne",
    properties=properties
)

dim_usage = spark.read.jdbc(
    jdbc_dwh,
    "dwh.dim_usage",
    properties=properties
)
releve = spark.read.jdbc(
    jdbc_staging,
    "staging.releve_compteur",
    properties=properties
)


dim_geo = spark.read.jdbc(
    jdbc_dwh,
    "dwh.dim_geographie",
    properties=properties
)


# ==========================
# MESURES
# ==========================

factures = factures.withColumn(
    "nb_factures",
    lit(1)
)

factures = factures.withColumn(
    "facture_payee",
    when(col("payee") == True, 1).otherwise(0)
)

factures = factures.withColumn(
    "facture_partielle",
    when(col("paiement_partiel") == True, 1).otherwise(0)
)

# ==========================
# JOINTURES
f = factures.alias("f")
d = dim_date.alias("d")
a = dim_abonne.alias("a")
u = dim_usage.alias("u")
r = releve.alias("r")
g = dim_geo.alias("g")

fact = f.join(
    d,
    f["date_facturation"] == d["date_complete"],
    "left"
)

fact = fact.join(
    a,
    f["numero_abonne"] == a["num_abonne"],
    "left"
)

fact = fact.join(
    u,
    a["usage_id"] == u["usage_id"],
    "left"
)
fact = fact.join(
    r,
    f["id_releve"] == r["id"],
    "left"
)
fact = fact.join(
    g,
    r["site_id"] == g["site_id"],
    "left"
)
# ==========================
# TABLE FINALE
# ==========================

fact_facturation = fact.select(

    col("d.date_key"),

    col("g.geo_key"),

    col("a.abonne_key"),

    col("u.usage_key"),
    col("f.num_facture"),
    col("f.numero_abonne"),
    col("f.volume").alias("volume_facture"),

    col("f.montant_ht"),

    col("f.montant_tva"),

    col("f.montant_surtaxe"),

    col("f.montant_ttc"),

    col("f.solde_ant"),

    col("f.montant_a_payer"),
    col("f.date_facturation"),
    col("f.date_echeance"),

    col("f.statut").alias("statut_facture"),
    col("f.payee"),
    col("f.paiement_partiel"),

    col("f.nb_factures"),

    col("f.facture_payee"),

    col("f.facture_partielle")
)
print("Nombre lignes :", fact_facturation.count())

fact_facturation.show(
    20,
    False
)
fact_facturation = fact_facturation.dropDuplicates()
# ==========================
# CHARGEMENT DWH
# ==========================

fact_facturation.write \
    .mode("overwrite") \
    .option("truncate", "true") \
    .jdbc(
        jdbc_dwh,
        "dwh.fact_facturation",
        properties=properties
    )

print("FACT_FACTURATION créée avec succès")

spark.stop()