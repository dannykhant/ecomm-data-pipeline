from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

@dp.view(name="stg_sales")
def stg_sales():
    df_sales_stream = dp.read_stream("brz_orders").alias("od") \
        .join(dp.read_stream("brz_order_items").alias("oi"), on="order_id") \
        .select("order_id",
                "customer_id",
                "product_id",
                "unit_price",
                "quantity",
                "item_price",
                "total_amount",
                "oi.created_at",
                "oi.updated_at") \
        .withColumn("_dp_loaded_at", current_timestamp())
    return df_sales_stream

@dp.table(name="silver.fact_sales")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
def fact_sales():
    df_fact = dp.read_stream("stg_sales").alias("s") \
        .join(dp.read("silver.dim_orders").alias("o"), on="order_id", how="left") \
        .join(dp.read("silver.dim_products").alias("p"), on="product_id", how="left") \
        .join(dp.read("silver.dim_customers").alias("c"), on="customer_id", how="left")

    return df_fact.select(
                    col("o.order_sk").cast("bigint").alias("order_sk"),
                    col("c.customer_sk").cast("bigint").alias("customer_sk"),
                    col("p.product_sk").cast("bigint").alias("product_sk"),
                    col("s.unit_price").cast("decimal(10,2)").alias("unit_price"),
                    col("s.quantity").cast("int").alias("quantity"),
                    col("s.item_price").cast("decimal(10,2)").alias("item_price"),
                    col("s.total_amount").cast("decimal(10,2)").alias("total_amount"),
                    col("s.created_at").cast("timestamp").alias("created_at"),
                    col("s.updated_at").cast("timestamp").alias("updated_at"),
                    col("s._dp_loaded_at").cast("timestamp").alias("_dp_loaded_at")
                )
