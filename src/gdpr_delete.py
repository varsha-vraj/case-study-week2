from config import get_spark_session
from dv_utils import generate_hash

# Start Spark session
spark = get_spark_session()

# Step 1: Define which customer to delete
target_customer_id = "C002"

# Step 2: Generate corresponding Customer_HK
df = spark.createDataFrame([[target_customer_id]], ["customer_id"])
df = generate_hash(df, ["customer_id"], "Customer_HK")
customer_hash = df.collect()[0]["Customer_HK"]

# Step 3: Delete from sat_customer where hash matches
table_path = "file:///C:/Users/varsha.v/Desktop/ecommerce-data-vault/delta_tables/sat_customer"

spark.sql(f"""
    DELETE FROM delta.`{table_path}`
    WHERE Customer_HK = '{customer_hash}'
""")

print(f"GDPR delete successful: Customer {target_customer_id} removed from sat_customer")
