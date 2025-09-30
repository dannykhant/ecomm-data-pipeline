from pyspark import pipelines as dp
from pyspark.sql.functions import sum, col


@dp.table(name="gold.agg_daily_sales")
def agg_daily_sales():
    df = dp.read("gold.sales_analytics") \
        .withColumn("order_date", col("order_date").cast("date")) \
        .groupBy("order_date") \
        .agg(sum(col("item_price")).alias("total_sales"))
    return df