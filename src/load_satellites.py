from config import get_spark_session
from dv_utils import generate_hash, add_audit_columns

# Start Spark session
spark = get_spark_session()

# Read customers.csv
df_customers = spark.read.option("header", True).csv("data/customers.csv")

# Select descriptive fields
df_sat_customer = df_customers.select("customer_id", "name", "email", "phone")

# Generate hash key to link with Hub
df_sat_customer = generate_hash(df_sat_customer, ["customer_id"], "Customer_HK")

# Add audit columns
df_sat_customer = add_audit_columns(df_sat_customer)

# Reorder columns (optional)
cols = ["Customer_HK", "name", "email", "phone", "Load_Date", "Record_Source"]
df_sat_customer = df_sat_customer.select(*cols)

# Write to Delta table
df_sat_customer.write.format("delta").mode("overwrite").save("delta_tables/sat_customer")

print("Sat_Customer table written to delta_tables/sat_customer")

# Sat_Product 

# Read products.csv
df_products = spark.read.option("header", True).csv("data/products.csv")

# Select descriptive fields
df_sat_product = df_products.select("product_id", "product_name", "category", "price")

# Generate hash key to link with Hub
df_sat_product = generate_hash(df_sat_product, ["product_id"], "Product_HK")

# Add audit columns
df_sat_product = add_audit_columns(df_sat_product)

# Reorder columns
cols = ["Product_HK", "product_name", "category", "price", "Load_Date", "Record_Source"]
df_sat_product = df_sat_product.select(*cols)

# Write to Delta table
df_sat_product.write.format("delta").mode("overwrite").save("delta_tables/sat_product")

print("Sat_Product table written to delta_tables/sat_product")

#  Sat_Order 

# Read orders.csv
df_orders = spark.read.option("header", True).csv("data/orders.csv")

# Select descriptive fields
df_sat_order = df_orders.select("order_id", "order_date", "order_status")

# Generate hash key to link with Hub
df_sat_order = generate_hash(df_sat_order, ["order_id"], "Order_HK")

# Add audit columns
df_sat_order = add_audit_columns(df_sat_order)

# Reorder columns
cols = ["Order_HK", "order_date", "order_status", "Load_Date", "Record_Source"]
df_sat_order = df_sat_order.select(*cols)

# Write to Delta table
df_sat_order.write.format("delta").mode("overwrite").save("delta_tables/sat_order")

print("Sat_Order table written to delta_tables/sat_order")
