from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit
)

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("FACT_PAIEMENT") \
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

paiements = spark.read.jdbc(
    jdbc_staging,
    "staging.paiements",
    properties=properties
)

# ==========================
# DIMENSIONS
# ==========================

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

dim_mode = spark.read.jdbc(
    jdbc_dwh,
    "dwh.dim_mode_paiement",
    properties=properties
)
factures = spark.read.jdbc(
    jdbc_staging,
    "staging.factures",
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

paiements = paiements.withColumn(
    "nb_paiements",
    lit(1)
)

paiements = paiements.withColumn(
    "paiement_valide",
    when(
        col("statut") == "VALIDE",
        1
    ).otherwise(0)
)

# ==========================
# ALIAS
# ==========================

p = paiements.alias("p")
d = dim_date.alias("d")
a = dim_abonne.alias("a")
m = dim_mode.alias("m")
f = factures.alias("f")
r = releve.alias("r")
g = dim_geo.alias("g")

# ==========================
# JOINTURE DATE
# ==========================

fact = p.join(
    d,
    p["date_paiement"] == d["date_complete"],
    "left"
)

# ==========================
# JOINTURE ABONNE
# ==========================

fact = fact.join(
    a,
    p["numero_abonne"] == a["num_abonne"],
    "left"
)

# ==========================
# JOINTURE MODE PAIEMENT
# ==========================

fact = fact.join(
    m,
    (
        (p["mode_paiement"] == m["mode_paiement"])
        &
        (p["type_caisse"] == m["type_caisse"])
    ),
    "left"
)
# ==========================
# JOINTURE FACTURE
# ==========================

fact = fact.join(
    f,
    (
        (p["num_facture"] == f["num_facture"])
    ),
    "left"
)

# ==========================
# JOINTURE RELEVE
# ==========================

fact = fact.join(
    r,
    f["id_releve"] == r["id"],
    "left"
)

# ==========================
# JOINTURE GEOGRAPHIE
# ==========================

fact = fact.join(
    g,
    r["site_id"] == g["site_id"],
    "left"
)
# ==========================
# TABLE FINALE
# ==========================

fact_paiement = fact.select(

    col("d.date_key").alias("date_key"),

    col("g.geo_key").alias("geo_key"),

    col("a.abonne_key").alias("abonne_key"),

    col("m.mode_paiement_key").alias("mode_paiement_key"),

    col("p.num_facture"),

    col("p.numero_abonne"),

    col("p.date_paiement"),

    col("p.montant").alias("montant_encaisse"),

    col("p.statut"),

    col("p.nb_paiements"),
    col("p.mode_paiement"),

    col("p.type_caisse"),
    col("p.paiement_valide")
)
print("Nombre lignes :", fact_paiement.count())

fact_paiement.show(
    20,
    False
)
fact_paiement = fact_paiement.dropDuplicates(["num_facture"])
# ==========================
# CHARGEMENT DWH

fact_paiement.write \
    .mode("overwrite") \
    .option("truncate", "true") \
    .jdbc(
        jdbc_dwh,
        "dwh.fact_paiement",
        properties=properties
    )
print("FACT_PAIEMENT créée avec succès")

spark.stop()