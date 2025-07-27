from config import get_spark_session
from dv_utils import generate_hash, add_audit_columns
from pyspark.sql.functions import lit, col, current_date
from pyspark.sql import Window
import pyspark.sql.functions as F

# Start Spark session
spark = get_spark_session()

#Read current SCD table if exists
try:
    scd_existing = spark.read.format("delta").load("delta_tables/scd_sat_customer")
except:
    scd_existing = spark.createDataFrame([], schema="""
        Customer_HK STRING, name STRING, email STRING, phone STRING,
        Start_Date DATE, End_Date DATE, Is_Current BOOLEAN,
        Load_Date TIMESTAMP, Record_Source STRING
    """)

# Read latest raw customer data
df = spark.read.option("header", True).csv("data/customers.csv")

# Create new version with hash key
df = generate_hash(df, ["customer_id"], "Customer_HK")
df = add_audit_columns(df)
df = df.withColumn("Start_Date", current_date()) \
       .withColumn("End_Date", lit("9999-12-31").cast("date")) \
       .withColumn("Is_Current", lit(True))

# Columns to compare
compare_cols = ["name", "email", "phone"]

# Join new vs existing on Customer_HK
joined = df.alias("new").join(
    scd_existing.filter("Is_Current = true").alias("old"),
    on="Customer_HK",
    how="left"
)

# Detect changes (row where any attribute changed)
changes = joined.filter(
    F.coalesce(col("new.name") != col("old.name"), lit(False)) |
    F.coalesce(col("new.email") != col("old.email"), lit(False)) |
    F.coalesce(col("new.phone") != col("old.phone"), lit(False))
)

# Select only required SCD columns
changes = changes.select(
    "new.Customer_HK", "new.name", "new.email", "new.phone",
    "new.Start_Date", "new.End_Date", "new.Is_Current",
    "new.Load_Date", "new.Record_Source"
)

# Expire old records by setting End_Date and Is_Current = False
expired = joined.filter(changes["Customer_HK"].isNotNull()) \
    .select("old.*") \
    .withColumn("End_Date", current_date()) \
    .withColumn("Is_Current", lit(False))

# Union all: unchanged + expired + new
unchanged = scd_existing.filter("Is_Current = true").join(
    changes.select("Customer_HK"), on="Customer_HK", how="left_anti"
)
final_df = unchanged.unionByName(expired).unionByName(changes)

# Save result
final_df.write.format("delta").mode("overwrite").save("delta_tables/scd_sat_customer")

print(" SCD Type 2 applied on sat_customer → saved to scd_sat_customer")



