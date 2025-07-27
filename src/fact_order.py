from config import get_spark_session
from pyspark.sql import functions as F

spark = get_spark_session()
base = "file:///C:/Users/varsha.v/Desktop/ecommerce-data-vault/delta_tables"

# Load all required tables
link_order_product = spark.read.format("delta").load(f"{base}/link_order_product")
link_order_customer = spark.read.format("delta").load(f"{base}/link_customer_order")
sat_order = spark.read.format("delta").load(f"{base}/sat_order")
dim_customer = spark.read.format("delta").load(f"{base}/dim_customer").filter("Is_Current = true")
dim_product = spark.read.format("delta").load(f"{base}/dim_product").filter("Is_Current = true")

# Join both links on Order_HK
link_joined = link_order_product.join(link_order_customer, on="Order_HK")

# Add order details
fact_df = link_joined.join(sat_order, on="Order_HK", how="left") \
    .join(dim_customer, on="Customer_HK", how="left") \
    .join(dim_product, on="Product_HK", how="left") \
    .select(
        "Order_HK",
        "Customer_Key",
        "Product_Key",
        "Order_Date",
        "Order_Status"
    )

# Save fact table
fact_df.write.format("delta").mode("overwrite").save(f"{base}/fact_order")
print(" fact_order table created.")
