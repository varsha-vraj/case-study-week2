from config import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = get_spark_session()
base = "file:///C:/Users/varsha.v/Desktop/ecommerce-data-vault/delta_tables"

# Load current version of product SCD
scd_df = spark.read.format("delta").load(f"{base}/scd_sat_product") \
    .filter("Is_Current = true")

# Generate surrogate key
window_spec = Window.orderBy("Product_HK")
dim_df = scd_df.withColumn("Product_Key", F.row_number().over(window_spec)) \
    .select(
        "Product_Key",
        "Product_HK",
        "Product_Name",
        "Category",
        "Price",
        "Start_Date",
        "End_Date",
        "Is_Current"
    )

# Save dimension table
dim_df.write.format("delta").mode("overwrite").save(f"{base}/dim_product")
print(" dim_product table created.")
