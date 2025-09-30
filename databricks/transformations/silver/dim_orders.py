from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp


@dp.view(name="stg_orders")
def stg_orders():
    df = dp.read_stream("brz_orders")
    df = df.withColumn("order_id", col("order_id").cast("string")) \
            .withColumn("order_date", col("order_date").cast("timestamp")) \
            .withColumn("order_status", col("status").cast("string")) \
            .withColumn("created_at", col("created_at").cast("timestamp")) \
            .withColumn("updated_at", col("updated_at").cast("timestamp")) \
            .withColumn("_dp_loaded_at", current_timestamp())

    return df.select(
        "order_id",
        "order_date",
        "order_status",
        "created_at",
        "updated_at",
        "_dp_loaded_at"
    )

dp.create_streaming_table(
    name="silver.dim_orders",
    schema="""
        order_sk bigint generated always as identity,
        order_id string not null,
        order_date timestamp,
        order_status string,
        created_at timestamp not null,
        updated_at timestamp not null,
        _dp_loaded_at timestamp not null
    """,
    expect_all_or_drop={"valid_id": "order_id IS NOT NULL"},
    expect_all={
        "valid_order_status": "order_status in ('shipped', 'delivered', 'cancelled', 'pending')"
    }
)

dp.create_auto_cdc_flow(
  target = "silver.dim_orders",
  source = "stg_orders",
  keys = ["order_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 1,
  name = "silver_orders_cdc_flow"
)