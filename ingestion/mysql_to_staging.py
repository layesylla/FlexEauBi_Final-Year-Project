import dlt
import pandas as pd
from sqlalchemy import create_engine
from config import TABLES
mysql_engine = create_engine(
    "mysql+pymysql://root@host.docker.internal:3306/commerciale_flex_db"
)
#les ressources de dtl
def extract_table(table_name):

    @dlt.resource(
        name=table_name,
        write_disposition="replace"
    )
    def resource():

        query = f"SELECT * FROM {table_name}"

        df = pd.read_sql(query, mysql_engine)

        yield df.to_dict("records")
    return resource
#conf du pipeline de chargemmt
pipeline = dlt.pipeline(
    pipeline_name="flex_staging_pipeline",
    destination="postgres",
    dataset_name="staging"
)
#Le chargement de toutes les tables
for table in TABLES:
    print(f"\n===== {table} =====")
    load_info = pipeline.run(
        extract_table(table)()
    )
    print(load_info)