import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col,
    datediff,
    round as spark_round,
    current_timestamp
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

database_name = "supply_chain_processed_d"
curated_base_path = "s3://satish-supply-chain-data/curated/"


# -------------------------
# READ PROCESSED TABLES
# -------------------------

orders = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="orders"
).toDF()

order_items = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="order_items"
).toDF()

customers = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="customers"
).toDF()

products = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="products"
).toDF()

warehouses = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="warehouses"
).toDF()

shipments = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="shipments"
).toDF()


# -------------------------
# CURATED ORDER ANALYTICS
# -------------------------

order_analytics = (
    orders.alias("o")
    .join(
        order_items.alias("oi"),
        col("o.order_id") == col("oi.order_id"),
        "inner"
    )
    .join(
        customers.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"
    )
    .join(
        products.alias("p"),
        col("oi.product_id") == col("p.product_id"),
        "left"
    )
    .join(
        warehouses.alias("w"),
        col("o.warehouse_id") == col("w.warehouse_id"),
        "left"
    )
    .join(
        shipments.alias("s"),
        col("o.order_id") == col("s.order_id"),
        "left"
    )
    .select(
        col("o.order_id"),
        col("o.order_date"),
        col("o.order_status"),

        col("c.customer_id"),
        col("c.customer_name"),
        col("c.customer_segment"),

        col("w.warehouse_id"),
        col("w.warehouse_name"),
        col("w.city").alias("warehouse_city"),

        col("p.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("p.supplier_id"),

        col("oi.quantity"),
        col("oi.unit_price"),

        spark_round(
            col("oi.quantity") * col("oi.unit_price"),
            2
        ).alias("line_revenue"),

        col("s.shipment_id"),
        col("s.carrier"),
        col("s.shipment_date"),
        col("s.delivery_date"),
        col("s.shipment_status"),

        datediff(
            col("s.delivery_date"),
            col("s.shipment_date")
        ).alias("delivery_days"),

        current_timestamp().alias("curated_timestamp")
    )
)


# -------------------------
# WRITE CURATED DATA
# -------------------------

order_analytics.write \
    .mode("overwrite") \
    .parquet(curated_base_path + "order_analytics/")


job.commit()
