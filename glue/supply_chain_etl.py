# AWS Glue PySpark ETL Job
# Supply Chain Analytics Pipeline
import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col,
    to_date,
    current_timestamp,
    trim,
    upper,
    when,
    lit,
    concat_ws,
    input_file_name,
    count as spark_count
)


# ==================================================
# INITIALIZE GLUE JOB
# ==================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ==================================================
# CONFIGURATION
# ==================================================

database_name = "supply_chain_raw_db"

processed_base_path = (
    "s3://satish-supply-chain-data/processed/"
)

rejected_base_path = (
    "s3://satish-supply-chain-data/rejected/"
)


# ==================================================
# HELPER FUNCTIONS
# ==================================================

def normalize_blank_strings(df, columns):
    """
    Converts:
        ""
        "   "
    into NULL.

    Also trims normal string values.
    """

    for column_name in columns:

        df = df.withColumn(
            column_name,
            when(
                col(column_name).isNull(),
                lit(None).cast("string")
            )
            .when(
                trim(col(column_name)) == "",
                lit(None).cast("string")
            )
            .otherwise(
                trim(col(column_name))
            )
        )

    return df


def add_duplicate_count(df, key_column):

    duplicate_window = Window.partitionBy(key_column)

    return df.withColumn(
        "_duplicate_count",
        spark_count("*").over(duplicate_window)
    )


def add_rejection_metadata(df, rules):

    reason_columns = [
        when(condition, lit(reason))
        for condition, reason in rules
    ]

    return (
        df
        .withColumn(
            "rejection_reason",
            concat_ws("; ", *reason_columns)
        )
        .withColumn(
            "rejected_timestamp",
            current_timestamp()
        )
    )


# ==================================================
# 1. CUSTOMERS
# ==================================================

customers_dyf = (
    glueContext
    .create_dynamic_frame
    .from_catalog(
        database=database_name,
        table_name="customers"
    )
)

customers_raw = (
    customers_dyf
    .toDF()
    .withColumn(
        "source_file",
        input_file_name()
    )
)

customers_raw = normalize_blank_strings(
    customers_raw,
    [
        "customer_id",
        "customer_name",
        "city",
        "state",
        "customer_segment"
    ]
)

customers_checked = add_duplicate_count(
    customers_raw,
    "customer_id"
)


customer_rules = [

    (
        col("customer_id").isNull(),
        "MISSING_CUSTOMER_ID"
    ),

    (
        col("customer_name").isNull(),
        "MISSING_CUSTOMER_NAME"
    ),

    (
        col("customer_id").isNotNull()
        &
        (col("_duplicate_count") > 1),
        "DUPLICATE_CUSTOMER_ID"
    )
]


customer_valid_condition = (

    col("customer_id").isNotNull()
    &
    col("customer_name").isNotNull()
    &
    (col("_duplicate_count") == 1)
)


customers_valid = (

    customers_checked

    .filter(customer_valid_condition)

    .withColumn(
        "state",
        upper(col("state"))
    )

    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )

    .drop(
        "_duplicate_count",
        "source_file"
    )
)


customers_rejected = (
    customers_checked
    .filter(~customer_valid_condition)
)

customers_rejected = add_rejection_metadata(
    customers_rejected,
    customer_rules
)

customers_rejected = (
    customers_rejected
    .drop("_duplicate_count")
)


# ==================================================
# 2. PRODUCTS
# ==================================================

products_dyf = (
    glueContext
    .create_dynamic_frame
    .from_catalog(
        database=database_name,
        table_name="products"
    )
)

products_raw = (
    products_dyf
    .toDF()
    .withColumn(
        "source_file",
        input_file_name()
    )
)

products_raw = normalize_blank_strings(
    products_raw,
    [
        "product_id",
        "product_name",
        "category",
        "supplier_id"
    ]
)

products_checked = add_duplicate_count(
    products_raw,
    "product_id"
)


product_rules = [

    (
        col("product_id").isNull(),
        "MISSING_PRODUCT_ID"
    ),

    (
        col("product_name").isNull(),
        "MISSING_PRODUCT_NAME"
    ),

    (
        col("supplier_id").isNull(),
        "MISSING_SUPPLIER_ID"
    ),

    (
        col("unit_cost").isNull(),
        "MISSING_UNIT_COST"
    ),

    (
        col("unit_cost") < 0,
        "NEGATIVE_UNIT_COST"
    ),

    (
        col("unit_price").isNull(),
        "MISSING_UNIT_PRICE"
    ),

    (
        col("unit_price") < 0,
        "NEGATIVE_UNIT_PRICE"
    ),

    (
        col("product_id").isNotNull()
        &
        (col("_duplicate_count") > 1),
        "DUPLICATE_PRODUCT_ID"
    )
]


product_valid_condition = (

    col("product_id").isNotNull()
    &
    col("product_name").isNotNull()
    &
    col("supplier_id").isNotNull()
    &
    col("unit_cost").isNotNull()
    &
    (col("unit_cost") >= 0)
    &
    col("unit_price").isNotNull()
    &
    (col("unit_price") >= 0)
    &
    (col("_duplicate_count") == 1)
)


products_valid = (

    products_checked

    .filter(product_valid_condition)

    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )

    .drop(
        "_duplicate_count",
        "source_file"
    )
)


products_rejected = (
    products_checked
    .filter(~product_valid_condition)
)

products_rejected = add_rejection_metadata(
    products_rejected,
    product_rules
)

products_rejected = (
    products_rejected
    .drop("_duplicate_count")
)


# ==================================================
# 3. SUPPLIERS
# ==================================================

suppliers_dyf = (
    glueContext
    .create_dynamic_frame
    .from_catalog(
        database=database_name,
        table_name="suppliers"
    )
)

suppliers_raw = (
    suppliers_dyf
    .toDF()
    .withColumn(
        "source_file",
        input_file_name()
    )
)

suppliers_raw = normalize_blank_strings(
    suppliers_raw,
    [
        "supplier_id",
        "supplier_name",
        "country"
    ]
)

suppliers_checked = add_duplicate_count(
    suppliers_raw,
    "supplier_id"
)


supplier_rules = [

    (
        col("supplier_id").isNull(),
        "MISSING_SUPPLIER_ID"
    ),

    (
        col("supplier_name").isNull(),
        "MISSING_SUPPLIER_NAME"
    ),

    (
        col("supplier_rating").isNull(),
        "MISSING_SUPPLIER_RATING"
    ),

    (
        col("supplier_rating") < 0,
        "SUPPLIER_RATING_BELOW_ZERO"
    ),

    (
        col("supplier_rating") > 5,
        "SUPPLIER_RATING_ABOVE_FIVE"
    ),

    (
        col("supplier_id").isNotNull()
        &
        (col("_duplicate_count") > 1),
        "DUPLICATE_SUPPLIER_ID"
    )
]


supplier_valid_condition = (

    col("supplier_id").isNotNull()
    &
    col("supplier_name").isNotNull()
    &
    col("supplier_rating").isNotNull()
    &
    (col("supplier_rating") >= 0)
    &
    (col("supplier_rating") <= 5)
    &
    (col("_duplicate_count") == 1)
)


suppliers_valid = (

    suppliers_checked

    .filter(supplier_valid_condition)

    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )

    .drop(
        "_duplicate_count",
        "source_file"
    )
)


suppliers_rejected = (
    suppliers_checked
    .filter(~supplier_valid_condition)
)

suppliers_rejected = add_rejection_metadata(
    suppliers_rejected,
    supplier_rules
)

suppliers_rejected = (
    suppliers_rejected
    .drop("_duplicate_count")
)


# ==================================================
# 4. WAREHOUSES
# ==================================================

warehouses_dyf = (
    glueContext
    .create_dynamic_frame
    .from_catalog(
        database=database_name,
        table_name="warehouses"
    )
)

warehouses_raw = (
    warehouses_dyf
    .toDF()
    .withColumn(
        "source_file",
        input_file_name()
    )
)

warehouses_raw = normalize_blank_strings(
    warehouses_raw,
    [
        "warehouse_id",
        "warehouse_name",
        "city"
    ]
)

warehouses_checked = add_duplicate_count(
    warehouses_raw,
    "warehouse_id"
)


warehouse_rules = [

    (
        col("warehouse_id").isNull(),
        "MISSING_WAREHOUSE_ID"
    ),

    (
        col("warehouse_name").isNull(),
        "MISSING_WAREHOUSE_NAME"
    ),

    (
        col("capacity_units").isNull(),
        "MISSING_CAPACITY"
    ),

    (
        col("capacity_units") <= 0,
        "INVALID_CAPACITY"
    ),

    (
        col("warehouse_id").isNotNull()
        &
        (col("_duplicate_count") > 1),
        "DUPLICATE_WAREHOUSE_ID"
    )
]


warehouse_valid_condition = (

    col("warehouse_id").isNotNull()
    &
    col("warehouse_name").isNotNull()
    &
    col("capacity_units").isNotNull()
    &
    (col("capacity_units") > 0)
    &
    (col("_duplicate_count") == 1)
)


warehouses_valid = (

    warehouses_checked

    .filter(warehouse_valid_condition)

    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )

    .drop(
        "_duplicate_count",
        "source_file"
    )
)


warehouses_rejected = (
    warehouses_checked
    .filter(~warehouse_valid_condition)
)

warehouses_rejected = add_rejection_metadata(
    warehouses_rejected,
    warehouse_rules
)

warehouses_rejected = (
    warehouses_rejected
    .drop("_duplicate_count")
)


# ==================================================
# 5. INVENTORY
# ==================================================

inventory_dyf = (
    glueContext
    .create_dynamic_frame
    .from_catalog(
        database=database_name,
        table_name="inventory"
    )
)

inventory_raw = (
    inventory_dyf
    .toDF()
    .withColumn(
        "source_file",
        input_file_name()
    )
)

inventory_raw = normalize_blank_strings(
    inventory_raw,
    [
        "inventory_id",
        "warehouse_id",
        "product_id",
        "inventory_date"
    ]
)

inventory_checked = (

    add_duplicate_count(
        inventory_raw,
        "inventory_id"
    )

    .withColumn(
        "_parsed_inventory_date",
        to_date(
            col("inventory_date"),
            "yyyy-MM-dd"
        )
    )
)


inventory_rules = [

    (
        col("inventory_id").isNull(),
        "MISSING_INVENTORY_ID"
    ),

    (
        col("warehouse_id").isNull(),
        "MISSING_WAREHOUSE_ID"
    ),

    (
        col("product_id").isNull(),
        "MISSING_PRODUCT_ID"
    ),

    (
        col("quantity_on_hand").isNull(),
        "MISSING_QUANTITY_ON_HAND"
    ),

    (
        col("quantity_on_hand") < 0,
        "NEGATIVE_QUANTITY_ON_HAND"
    ),

    (
        col("reorder_level").isNull(),
        "MISSING_REORDER_LEVEL"
    ),

    (
        col("reorder_level") < 0,
        "NEGATIVE_REORDER_LEVEL"
    ),

    (
        col("_parsed_inventory_date").isNull(),
        "INVALID_INVENTORY_DATE"
    ),

    (
        col("inventory_id").isNotNull()
        &
        (col("_duplicate_count") > 1),
        "DUPLICATE_INVENTORY_ID"
    )
]


inventory_valid_condition = (

    col("inventory_id").isNotNull()
    &
    col("warehouse_id").isNotNull()
    &
    col("product_id").isNotNull()
    &
    col("quantity_on_hand").isNotNull()
    &
    (col("quantity_on_hand") >= 0)
    &
    col("reorder_level").isNotNull()
    &
    (col("reorder_level") >= 0)
    &
    col("_parsed_inventory_date").isNotNull()
    &
    (col("_duplicate_count") == 1)
)


inventory_valid = (

    inventory_checked

    .filter(inventory_valid_condition)

    .withColumn(
        "inventory_date",
        col("_parsed_inventory_date")
    )

    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )

    .drop(
        "_duplicate_count",
        "_parsed_inventory_date",
        "source_file"
    )
)


inventory_rejected = (
    inventory_checked
    .filter(~inventory_valid_condition)
)

inventory_rejected = add_rejection_metadata(
    inventory_rejected,
    inventory_rules
)

inventory_rejected = (
    inventory_rejected
    .drop(
        "_duplicate_count",
        "_parsed_inventory_date"
    )
)


# ==================================================
# 6. ORDERS
# ==================================================

orders_dyf = (
    glueContext
    .create_dynamic_frame
    .from_catalog(
        database=database_name,
        table_name="orders"
    )
)

orders_raw = (
    orders_dyf
    .toDF()
    .withColumn(
        "source_file",
        input_file_name()
    )
)


# --------------------------------------------------
# IMPORTANT:
# Convert blank strings to NULL before validation
# --------------------------------------------------

orders_raw = normalize_blank_strings(
    orders_raw,
    [
        "order_id",
        "customer_id",
        "warehouse_id",
        "order_date",
        "order_status"
    ]
)


orders_checked = (

    add_duplicate_count(
        orders_raw,
        "order_id"
    )

    .withColumn(
        "_parsed_order_date",
        to_date(
            col("order_date"),
            "yyyy-MM-dd"
        )
    )
)


order_rules = [

    (
        col("order_id").isNull(),
        "MISSING_ORDER_ID"
    ),

    (
        col("customer_id").isNull(),
        "MISSING_CUSTOMER_ID"
    ),

    (
        col("warehouse_id").isNull(),
        "MISSING_WAREHOUSE_ID"
    ),

    (
        col("order_status").isNull(),
        "MISSING_ORDER_STATUS"
    ),

    (
        col("order_date").isNull(),
        "MISSING_ORDER_DATE"
    ),

    (
        col("order_date").isNotNull()
        &
        col("_parsed_order_date").isNull(),
        "INVALID_ORDER_DATE"
    ),

    (
        col("order_id").isNotNull()
        &
        (col("_duplicate_count") > 1),
        "DUPLICATE_ORDER_ID"
    )
]


order_valid_condition = (

    col("order_id").isNotNull()
    &
    col("customer_id").isNotNull()
    &
    col("warehouse_id").isNotNull()
    &
    col("order_status").isNotNull()
    &
    col("order_date").isNotNull()
    &
    col("_parsed_order_date").isNotNull()
    &
    (col("_duplicate_count") == 1)
)


orders_valid = (

    orders_checked

    .filter(order_valid_condition)

    .withColumn(
        "order_date",
        col("_parsed_order_date")
    )

    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )

    .drop(
        "_duplicate_count",
        "_parsed_order_date",
        "source_file"
    )
)


orders_rejected = (
    orders_checked
    .filter(~order_valid_condition)
)

orders_rejected = add_rejection_metadata(
    orders_rejected,
    order_rules
)

orders_rejected = (
    orders_rejected
    .drop(
        "_duplicate_count",
        "_parsed_order_date"
    )
)


# ==================================================
# 7. ORDER ITEMS
# ==================================================

order_items_dyf = (
    glueContext
    .create_dynamic_frame
    .from_catalog(
        database=database_name,
        table_name="order_items"
    )
)

order_items_raw = (
    order_items_dyf
    .toDF()
    .withColumn(
        "source_file",
        input_file_name()
    )
)

order_items_raw = normalize_blank_strings(
    order_items_raw,
    [
        "order_item_id",
        "order_id",
        "product_id"
    ]
)

order_items_checked = add_duplicate_count(
    order_items_raw,
    "order_item_id"
)


order_item_rules = [

    (
        col("order_item_id").isNull(),
        "MISSING_ORDER_ITEM_ID"
    ),

    (
        col("order_id").isNull(),
        "MISSING_ORDER_ID"
    ),

    (
        col("product_id").isNull(),
        "MISSING_PRODUCT_ID"
    ),

    (
        col("quantity").isNull(),
        "MISSING_QUANTITY"
    ),

    (
        col("quantity") <= 0,
        "INVALID_QUANTITY"
    ),

    (
        col("unit_price").isNull(),
        "MISSING_UNIT_PRICE"
    ),

    (
        col("unit_price") < 0,
        "NEGATIVE_UNIT_PRICE"
    ),

    (
        col("order_item_id").isNotNull()
        &
        (col("_duplicate_count") > 1),
        "DUPLICATE_ORDER_ITEM_ID"
    )
]


order_item_valid_condition = (

    col("order_item_id").isNotNull()
    &
    col("order_id").isNotNull()
    &
    col("product_id").isNotNull()
    &
    col("quantity").isNotNull()
    &
    (col("quantity") > 0)
    &
    col("unit_price").isNotNull()
    &
    (col("unit_price") >= 0)
    &
    (col("_duplicate_count") == 1)
)


order_items_valid = (

    order_items_checked

    .filter(order_item_valid_condition)

    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )

    .drop(
        "_duplicate_count",
        "source_file"
    )
)


order_items_rejected = (
    order_items_checked
    .filter(~order_item_valid_condition)
)

order_items_rejected = add_rejection_metadata(
    order_items_rejected,
    order_item_rules
)

order_items_rejected = (
    order_items_rejected
    .drop("_duplicate_count")
)


# ==================================================
# 8. SHIPMENTS
# ==================================================

shipments_dyf = (
    glueContext
    .create_dynamic_frame
    .from_catalog(
        database=database_name,
        table_name="shipments"
    )
)

shipments_raw = (
    shipments_dyf
    .toDF()
    .withColumn(
        "source_file",
        input_file_name()
    )
)

shipments_raw = normalize_blank_strings(
    shipments_raw,
    [
        "shipment_id",
        "order_id",
        "warehouse_id",
        "carrier",
        "shipment_date",
        "delivery_date",
        "shipment_status"
    ]
)


shipments_checked = (

    add_duplicate_count(
        shipments_raw,
        "shipment_id"
    )

    .withColumn(
        "_parsed_shipment_date",
        to_date(
            col("shipment_date"),
            "yyyy-MM-dd"
        )
    )

    .withColumn(
        "_parsed_delivery_date",
        to_date(
            col("delivery_date"),
            "yyyy-MM-dd"
        )
    )
)


shipment_rules = [

    (
        col("shipment_id").isNull(),
        "MISSING_SHIPMENT_ID"
    ),

    (
        col("order_id").isNull(),
        "MISSING_ORDER_ID"
    ),

    (
        col("warehouse_id").isNull(),
        "MISSING_WAREHOUSE_ID"
    ),

    (
        col("shipment_date").isNull(),
        "MISSING_SHIPMENT_DATE"
    ),

    (
        col("shipment_date").isNotNull()
        &
        col("_parsed_shipment_date").isNull(),
        "INVALID_SHIPMENT_DATE"
    ),

    (
        col("delivery_date").isNotNull()
        &
        col("_parsed_delivery_date").isNull(),
        "INVALID_DELIVERY_DATE"
    ),

    (
        col("_parsed_delivery_date").isNotNull()
        &
        col("_parsed_shipment_date").isNotNull()
        &
        (
            col("_parsed_delivery_date")
            <
            col("_parsed_shipment_date")
        ),
        "DELIVERY_BEFORE_SHIPMENT"
    ),

    (
        col("shipment_id").isNotNull()
        &
        (col("_duplicate_count") > 1),
        "DUPLICATE_SHIPMENT_ID"
    )
]


shipment_valid_condition = (

    col("shipment_id").isNotNull()
    &
    col("order_id").isNotNull()
    &
    col("warehouse_id").isNotNull()
    &
    col("shipment_date").isNotNull()
    &
    col("_parsed_shipment_date").isNotNull()
    &
    (
        col("delivery_date").isNull()
        |
        col("_parsed_delivery_date").isNotNull()
    )
    &
    (
        col("_parsed_delivery_date").isNull()
        |
        (
            col("_parsed_delivery_date")
            >=
            col("_parsed_shipment_date")
        )
    )
    &
    (col("_duplicate_count") == 1)
)


shipments_valid = (

    shipments_checked

    .filter(shipment_valid_condition)

    .withColumn(
        "shipment_date",
        col("_parsed_shipment_date")
    )

    .withColumn(
        "delivery_date",
        col("_parsed_delivery_date")
    )

    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )

    .drop(
        "_duplicate_count",
        "_parsed_shipment_date",
        "_parsed_delivery_date",
        "source_file"
    )
)


shipments_rejected = (
    shipments_checked
    .filter(~shipment_valid_condition)
)

shipments_rejected = add_rejection_metadata(
    shipments_rejected,
    shipment_rules
)

shipments_rejected = (
    shipments_rejected
    .drop(
        "_duplicate_count",
        "_parsed_shipment_date",
        "_parsed_delivery_date"
    )
)


# ==================================================
# WRITE VALID DATA TO PROCESSED S3
# ==================================================

customers_valid.write \
    .mode("overwrite") \
    .parquet(
        processed_base_path + "customers/"
    )

products_valid.write \
    .mode("overwrite") \
    .parquet(
        processed_base_path + "products/"
    )

suppliers_valid.write \
    .mode("overwrite") \
    .parquet(
        processed_base_path + "suppliers/"
    )

warehouses_valid.write \
    .mode("overwrite") \
    .parquet(
        processed_base_path + "warehouses/"
    )

inventory_valid.write \
    .mode("overwrite") \
    .parquet(
        processed_base_path + "inventory/"
    )

orders_valid.write \
    .mode("overwrite") \
    .parquet(
        processed_base_path + "orders/"
    )

order_items_valid.write \
    .mode("overwrite") \
    .parquet(
        processed_base_path + "order_items/"
    )

shipments_valid.write \
    .mode("overwrite") \
    .parquet(
        processed_base_path + "shipments/"
    )


# ==================================================
# WRITE INVALID DATA TO REJECTED S3
# ==================================================

customers_rejected.write \
    .mode("overwrite") \
    .parquet(
        rejected_base_path + "customers/"
    )

products_rejected.write \
    .mode("overwrite") \
    .parquet(
        rejected_base_path + "products/"
    )

suppliers_rejected.write \
    .mode("overwrite") \
    .parquet(
        rejected_base_path + "suppliers/"
    )

warehouses_rejected.write \
    .mode("overwrite") \
    .parquet(
        rejected_base_path + "warehouses/"
    )

inventory_rejected.write \
    .mode("overwrite") \
    .parquet(
        rejected_base_path + "inventory/"
    )

orders_rejected.write \
    .mode("overwrite") \
    .parquet(
        rejected_base_path + "orders/"
    )

order_items_rejected.write \
    .mode("overwrite") \
    .parquet(
        rejected_base_path + "order_items/"
    )

shipments_rejected.write \
    .mode("overwrite") \
    .parquet(
        rejected_base_path + "shipments/"
    )


# ==================================================
# COMMIT JOB
# ==================================================

job.commit()
