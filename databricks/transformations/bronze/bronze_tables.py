from pyspark import pipelines as dp

s3_path = "s3://saanay-data-lake/ecomm_db_extracts"

sources = ["customers", "products", "order_items", "orders"]

def ingest_data(source):
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.schemaLocation", f"{s3_path}/_checkpoint/brz_{source}") \
        .option("header", "true") \
        .load(f"{s3_path}/daily_extracts/{source}")

for source in sources:
    @dp.table(name=f"brz_{source}")
    def create_table(source=source):
        return ingest_data(source)
