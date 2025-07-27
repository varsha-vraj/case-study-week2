from config import get_spark_session
from dv_utils import generate_hash, add_audit_columns
from pyspark.sql.functions import concat_ws

# Start Spark session
spark = get_spark_session()

# Link_Customer_Order 

# Read orders.csv
df_orders = spark.read.option("header", True).csv("data/orders.csv")

# Select customer_id and order_id
df_link = df_orders.select("customer_id", "order_id").dropDuplicates()

# Generate hash keys for both Hubs
df_link = generate_hash(df_link, ["customer_id"], "Customer_HK")
df_link = generate_hash(df_link, ["order_id"], "Order_HK")

# Create composite Link_HK (customer_id + order_id)
df_link = generate_hash(df_link, ["customer_id", "order_id"], "Link_HK")

# Add audit columns
df_link = add_audit_columns(df_link)

# Reorder columns
cols = ["Link_HK", "Customer_HK", "Order_HK", "Load_Date", "Record_Source"]
df_link = df_link.select(*cols)

# Write to Delta table
df_link.write.format("delta").mode("overwrite").save("delta_tables/link_customer_order")

print("Link_Customer_Order table written to delta_tables/link_customer_order")

# Link_Order_Product 


from pyspark.sql import Row

# Dummy order-product pairs
dummy_data = [
    Row(order_id="O1001", product_id="P101"),
    Row(order_id="O1001", product_id="P102"),
    Row(order_id="O1002", product_id="P103"),
    Row(order_id="O1003", product_id="P104"),
    Row(order_id="O1004", product_id="P101")
]

df_order_product = spark.createDataFrame(dummy_data)

# Generate Hub hash keys
df_order_product = generate_hash(df_order_product, ["order_id"], "Order_HK")
df_order_product = generate_hash(df_order_product, ["product_id"], "Product_HK")

# Generate Link_HK
df_order_product = generate_hash(df_order_product, ["order_id", "product_id"], "Link_HK")

# Add audit columns
df_order_product = add_audit_columns(df_order_product)

# Select columns
cols = ["Link_HK", "Order_HK", "Product_HK", "Load_Date", "Record_Source"]
df_order_product = df_order_product.select(*cols)

# Write to Delta
df_order_product.write.format("delta").mode("overwrite").save("delta_tables/link_order_product")

print("Link_Order_Product table written to delta_tables/link_order_product")
