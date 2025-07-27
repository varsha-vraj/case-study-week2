from config import get_spark_session
from pyspark.sql import functions as F

spark = get_spark_session()
base = "file:///C:/Users/varsha.v/Desktop/ecommerce-data-vault/delta_tables"

# Load current order data
df_new = spark.read.format("delta").load(f"{base}/sat_order")

# Load existing SCD table (if it exists)
try:
    df_old = spark.read.format("delta").load(f"{base}/scd_sat_order")
except:
    df_old = spark.createDataFrame([], df_new.schema
        .add("Start_Date", "timestamp")
        .add("End_Date", "timestamp")
        .add("Is_Current", "boolean")
    )

# Join on Order_HK
joined = df_new.alias("new").join(df_old.alias("old"), on="Order_HK", how="left")

# Unchanged
unchanged = joined.filter(
    (F.col("new.Order_Date") == F.col("old.Order_Date")) &
    (F.col("new.Order_Status") == F.col("old.Order_Status")) &
    (F.col("old.Is_Current") == True)
).select("old.*")

# Expired
expired = joined.filter(
    (F.col("old.Is_Current") == True) & (
        (F.col("new.Order_Date") != F.col("old.Order_Date")) |
        (F.col("new.Order_Status") != F.col("old.Order_Status"))
    )
).select(
    F.col("old.Order_HK"),
    F.col("old.Order_Date"),
    F.col("old.Order_Status"),
    F.col("old.Load_Date"),
    F.col("old.Record_Source"),
    F.col("old.Start_Date"),
    F.current_timestamp().alias("End_Date"),
    F.lit(False).alias("Is_Current")
)

# Changes
changes = joined.filter(
    (F.col("old.Is_Current") == True) & (
        (F.col("new.Order_Date") != F.col("old.Order_Date")) |
        (F.col("new.Order_Status") != F.col("old.Order_Status"))
    )
)

changes = changes.select(
    F.col("new.Order_HK"),
    F.col("new.Order_Date"),
    F.col("new.Order_Status"),
    F.col("new.Load_Date"),
    F.col("new.Record_Source")
).withColumn("Start_Date", F.current_timestamp()) \
 .withColumn("End_Date", F.lit(None).cast("timestamp")) \
 .withColumn("Is_Current", F.lit(True))

# Union
final_df = unchanged.unionByName(expired).unionByName(changes)

# Show and write
final_df.show(truncate=False)

final_df.write.format("delta").mode("overwrite").save(f"{base}/scd_sat_order")
print(" scd_sat_order table written.")
