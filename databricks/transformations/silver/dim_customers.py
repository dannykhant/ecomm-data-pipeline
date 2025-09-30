from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit, current_timestamp, concat


@dp.view(name="stg_customers")
def stg_customers():
    df = dp.read_stream("brz_customers")
    df = df.withColumn("customer_id", col("customer_id").cast("string")) \
            .withColumn("first_name", col("first_name").cast("string")) \
            .withColumn("last_name", col("last_name").cast("string")) \
            .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))) \
            .withColumn("email", col("email").cast("string")) \
            .withColumn("created_at", col("created_at").cast("timestamp")) \
            .withColumn("updated_at", col("updated_at").cast("timestamp")) \
            .withColumn("_dp_loaded_at", current_timestamp())

    return df.select(
        "customer_id",
        "first_name",
        "last_name",
        "full_name",
        "email",
        "created_at",
        "updated_at",
        "_dp_loaded_at"
    )

dp.create_streaming_table(
    name="silver.dim_customers",
    schema="""
        customer_sk bigint generated always as identity,
        customer_id string not null,
        first_name string,
        last_name string,
        full_name string,
        email string,
        created_at timestamp not null,
        updated_at timestamp not null,
        _dp_loaded_at timestamp not null,
        __START_AT timestamp,
        __END_AT timestamp
    """,
    expect_all_or_drop={"valid_id": "customer_id IS NOT NULL"}
)

dp.create_auto_cdc_flow(
  target = "silver.dim_customers",
  source = "stg_customers",
  keys = ["customer_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  name = "silver_customers_cdc_flow"
)