import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import get_spark_session

from dv_utils import generate_hash, add_audit_columns


# Initialize Spark session
spark = get_spark_session()

# Step 1: Read customers.csv
df_customers = spark.read.option("header", True).csv("data/customers.csv")

# Step 2: Remove duplicates (based on business key)
df_customers = df_customers.dropDuplicates(["customer_id"])

# Step 3: Generate hash key (Customer_HK)
df_customers = generate_hash(df_customers, ["customer_id"], "Customer_HK")

# Step 4: Add audit columns (Load_Date, Record_Source)
df_customers = add_audit_columns(df_customers)

# Step 5: Reorder columns (optional)
cols = ["Customer_HK", "customer_id", "Load_Date", "Record_Source"]
df_customers = df_customers.select(*cols)

# Step 6: Write to Delta table
df_customers.write.format("delta") \
    .mode("overwrite") \
    .save("delta_tables/hub_customer")

print(" Hub_Customer table written to delta_tables/hub_customer")


# Hub_Product 

# Step 1: Read products.csv
df_products = spark.read.option("header", True).csv("data/products.csv")
  
# Step 2: Remove duplicates based on product_id
df_products = df_products.dropDuplicates(["product_id"])

# Step 3: Generate hash key (Product_HK)
df_products = generate_hash(df_products, ["product_id"], "Product_HK")

# Step 4: Add audit columns
df_products = add_audit_columns(df_products)

# Step 5: Select final columns
cols = ["Product_HK", "product_id", "Load_Date", "Record_Source"]
df_products = df_products.select(*cols)

# Step 6: Write to Delta table
df_products.write.format("delta").mode("overwrite").save("delta_tables/hub_product")

print(" Hub_Product table written to delta_tables/hub_product")


#  Hub_Order 

# Step 1: Read orders.csv
df_orders = spark.read.option("header", True).csv("data/orders.csv")

# Step 2: Remove duplicates based on order_id
df_orders = df_orders.dropDuplicates(["order_id"])

# Step 3: Generate hash key (Order_HK)
df_orders = generate_hash(df_orders, ["order_id"], "Order_HK")

# Step 4: Add audit columns
df_orders = add_audit_columns(df_orders)

# Step 5: Select final columns
cols = ["Order_HK", "order_id", "Load_Date", "Record_Source"]
df_orders = df_orders.select(*cols)

# Step 6: Write to Delta table
df_orders.write.format("delta").mode("overwrite").save("delta_tables/hub_order")

print(" Hub_Order table written to delta_tables/hub_order")
