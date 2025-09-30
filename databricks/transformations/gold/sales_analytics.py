from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.table(name="gold.sales_analytics")
def sales_analytics():
    df_joined = dp.read("silver.fact_sales").alias("s") \
        .join(dp.read("silver.dim_orders").alias("o"),
              on=col("s.order_sk") == col("o.order_sk")) \
        .join(dp.read("silver.dim_products").alias("p"),
              on=(col("s.product_sk") == col("p.product_sk")) & (col("p.__END_AT").isNull())) \
        .join(dp.read("silver.dim_customers").alias("c"),
              on=(col("s.customer_sk") == col("c.customer_sk")) & (col("c.__END_AT").isNull()))

    return df_joined.select(
      col("o.order_id"),
      col("o.order_date"),
      col("o.order_status"),
      col("c.full_name"),
      col("c.email"),
      col("p.product_name"),
      col("p.category"),
      col("s.unit_price"),
      col("s.quantity"),
      col("s.item_price"),
      col("s.total_amount")
    )