from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder \
    .config("spark.jars", "/opt/jars/postgresql-42.7.8.jar") \
    .appName("DIM_TYPE_INCIDENT") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

jdbc_staging = "jdbc:postgresql://postgres-staging:5432/flex_staging"

jdbc_dwh = "jdbc:postgresql://postgres-dwh:5432/flex_dwh"

properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

incidents = spark.read.jdbc(
    jdbc_staging,
    "staging.incidents",
    properties=properties
)

dim_type_incident = incidents.select(
    "type_incident"
).distinct()

dim_type_incident = dim_type_incident.withColumn(
    "type_incident_key",
    monotonically_increasing_id()
)

dim_type_incident = dim_type_incident.select(
    "type_incident_key",
    "type_incident"
)

print("Nombre lignes :", dim_type_incident.count())

dim_type_incident.show(20, False)

dim_type_incident.write \
    .mode("overwrite") \
    .jdbc(
        jdbc_dwh,
        "dwh.dim_type_incident",
        properties=properties
    )

print("DIM_TYPE_INCIDENT créée avec succès")

spark.stop()