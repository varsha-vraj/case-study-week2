from config import get_spark_session
from pyspark.sql import functions as F

spark = get_spark_session()
base = "file:///C:/Users/varsha.v/Desktop/ecommerce-data-vault/delta_tables"

# Load latest product satellite data
df_new = spark.read.format("delta").load(f"{base}/sat_product")

# Load previous SCD product table if it exists
try:
    df_old = spark.read.format("delta").load(f"{base}/scd_sat_product")
except:
    df_old = spark.createDataFrame([], df_new.schema
        .add("Start_Date", "timestamp")
        .add("End_Date", "timestamp")
        .add("Is_Current", "boolean")
    )

# Join new and old on Product_HK
joined = df_new.alias("new").join(df_old.alias("old"), on="Product_HK", how="left")

# Records where nothing has changed
unchanged = joined.filter(
    (F.col("new.Product_Name") == F.col("old.Product_Name")) &
    (F.col("new.Category") == F.col("old.Category")) &
    (F.col("new.Price") == F.col("old.Price")) &
    (F.col("old.Is_Current") == True)
).select("old.*")

# Expire old records with changes
expired = joined.filter(
    (F.col("old.Is_Current") == True) & (
        (F.col("new.Product_Name") != F.col("old.Product_Name")) |
        (F.col("new.Category") != F.col("old.Category")) |
        (F.col("new.Price") != F.col("old.Price"))
    )
).select(
    F.col("old.Product_HK"),
    F.col("old.Product_Name"),
    F.col("old.Category"),
    F.col("old.Price"),
    F.col("old.Load_Date"),
    F.col("old.Record_Source"),
    F.col("old.Start_Date"),
    F.current_timestamp().alias("End_Date"),
    F.lit(False).alias("Is_Current")
)


# Add new version of changed records
changes = joined.filter(
    (F.col("old.Is_Current") == True) & (
        (F.col("new.Product_Name") != F.col("old.Product_Name")) |
        (F.col("new.Category") != F.col("old.Category")) |
        (F.col("new.Price") != F.col("old.Price"))
    )
)

changes = changes.select(
    F.col("new.Product_HK"),
    F.col("new.Product_Name"),
    F.col("new.Category"),
    F.col("new.Price"),
    F.col("new.Load_Date"),
    F.col("new.Record_Source")
).withColumn("Start_Date", F.current_timestamp()) \
 .withColumn("End_Date", F.lit(None).cast("timestamp")) \
 .withColumn("Is_Current", F.lit(True))

# Final union of unchanged + expired + new version
final_df = unchanged.unionByName(expired).unionByName(changes)

# Save as delta
final_df.write.format("delta").mode("overwrite").save(f"{base}/scd_sat_product")
print(" scd_sat_product table written.")
