from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp


@dp.view(name="stg_products")
def stg_products():
    df = dp.read_stream("brz_products")
    df = df.withColumn("product_id", col("product_id").cast("string")) \
            .withColumn("product_name", col("product_name").cast("string")) \
            .withColumn("price", col("price").cast("float")) \
            .withColumn("category", col("category").cast("string")) \
            .withColumn("created_at", col("created_at").cast("timestamp")) \
            .withColumn("updated_at", col("updated_at").cast("timestamp")) \
            .withColumn("_dp_loaded_at", current_timestamp())

    return df.select(
        "product_id",
        "product_name",
        "price",
        "category",
        "created_at",
        "updated_at",
        "_dp_loaded_at"
    )

dp.create_streaming_table(
    name="silver.dim_products",
    schema="""
        product_sk bigint generated always as identity,
        product_id string not null,
        product_name string,
        price float,
        category string,
        created_at timestamp not null,
        updated_at timestamp not null,
        _dp_loaded_at timestamp not null,
        __START_AT timestamp,
        __END_AT timestamp
    """,
    expect_all_or_drop={"valid_id": "product_id is not null"}
)

dp.create_auto_cdc_flow(
  target = "silver.dim_products",
  source = "stg_products",
  keys = ["product_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  name = "silver_products_cdc_flow"
)