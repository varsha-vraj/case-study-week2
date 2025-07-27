from config import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = get_spark_session()
base = "file:///C:/Users/varsha.v/Desktop/ecommerce-data-vault/delta_tables"

# Load latest SCD snapshot where Is_Current = True
scd_df = spark.read.format("delta").load(f"{base}/scd_sat_customer") \
    .filter("Is_Current = true")

# Add surrogate key using row_number over Customer_HK
window_spec = Window.orderBy("Customer_HK")
dim_df = scd_df.withColumn("Customer_Key", F.row_number().over(window_spec)) \
    .select(
        "Customer_Key",
        "Customer_HK",
        "name",
        "email",
        "phone",
        "Start_Date",
        "End_Date",
        "Is_Current"
    )

# Save dimension
dim_df.write.format("delta").mode("overwrite").save(f"{base}/dim_customer")
print(" dim_customer table created.")
