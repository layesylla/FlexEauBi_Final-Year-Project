from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DIM_GEOGRAPHIE") \
    .config(
        "spark.jars",
        "/opt/jars/postgresql-42.7.8.jar"
    ) \
    .getOrCreate()


# CONNEXIONS JDBC


jdbc_staging = "jdbc:postgresql://postgres-staging:5432/flex_staging"

jdbc_dwh = "jdbc:postgresql://postgres-dwh:5432/flex_dwh"

properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# LECTURE STAGING
regions = spark.read.jdbc(
    jdbc_staging,
"staging.regions",
    properties=properties
)

departements = spark.read.jdbc(
    jdbc_staging,
    "staging.departements",
    properties=properties
)

communes = spark.read.jdbc(
    jdbc_staging,
    "staging.communes",
    properties=properties
)

sites = spark.read.jdbc(
    jdbc_staging,
    "staging.sites",
    properties=properties
)


# CONSTRUCTION DIMENSION

dim_geo = (
    sites.alias("s")
    .join(
        communes.alias("c"),
        sites["commune_id"] == communes["commune_id"],
        "inner"
    )
    .join(
        departements.alias("d"),
        communes["departement_id"] == departements["departement_id"],
        "inner"
    )
    .join(
        regions.alias("r"),
        departements["region_id"] == regions["region_id"],
        "inner"
    )
    .select(
        regions["region_id"],
        regions["code"].alias("region_code"),
        regions["region"].alias("region_nom"),

        departements["departement_id"],
        departements["departement"].alias("departement_nom"),

        communes["commune_id"],
        communes["commune"].alias("commune_nom"),

        sites["id"].alias("site_id"),
        sites["nom"].alias("site_nom")
    )
)


# CLE SUBSTITUT


dim_geo = dim_geo.withColumn(
    "geo_key",
    monotonically_increasing_id()
)

dim_geo = dim_geo.select(
    "geo_key",

    "region_id",
    "region_code",
    "region_nom",

    "departement_id",
    "departement_nom",

    "commune_id",
    "commune_nom",

    "site_id",
    "site_nom"
)

print("Nombre lignes :", dim_geo.count())

dim_geo.show(20, False)

# ==========================
# CHARGEMENT DWH
# ==========================

dim_geo.write \
    .mode("overwrite") \
    .jdbc(
        jdbc_dwh,
        "dwh.dim_geographie",
        properties=properties
    )

print("DIM_GEOGRAPHIE créée avec succès")

spark.stop()