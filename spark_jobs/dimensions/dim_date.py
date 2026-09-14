from pyspark.sql import SparkSession
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DIM_DATE") \
    .config(
        "spark.jars",
        "/opt/jars/postgresql-42.7.8.jar"
    ) \
    .getOrCreate()

start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 12, 31)

rows = []

current = start_date

while current <= end_date:

    rows.append(
        (
            int(current.strftime("%Y%m%d")),
            current.strftime("%Y-%m-%d"),
            current.day,
            current.month,
            current.strftime("%B"),
            ((current.month - 1) // 3) + 1,
            current.year,
            current.strftime("%A"),
            current.isocalendar()[1]
        )
    )

    current += timedelta(days=1)

df = spark.createDataFrame(
    rows,
    [
        "date_key",
        "date_complete",
        "jour",
        "mois",
        "nom_mois",
        "trimestre",
        "annee",
        "jour_semaine",
        "semaine_annee"
    ]
)

print("Nombre lignes :", df.count())

df.show(5, False)

jdbc_url = "jdbc:postgresql://postgres-dwh:5432/flex_dwh"

properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

try:

    print("AVANT WRITE")

    df.write \
        .mode("overwrite") \
        .jdbc(
            jdbc_url,
            "dwh.dim_date",
            properties=properties
        )

    print("DIM_DATE créée avec succès")

except Exception as e:

    print("ERREUR WRITE :")
    print(str(e))

finally:

    spark.stop()