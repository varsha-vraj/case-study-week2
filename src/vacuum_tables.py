from config import get_spark_session

spark = get_spark_session()

# Path to your delta table
table_path = "file:///C:/Users/varsha.v/Desktop/ecommerce-data-vault/delta_tables/sat_order"

# Set safety override
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# VACUUM with 0-hour retention (clears all old files)
spark.sql(f"VACUUM delta.`{table_path}` RETAIN 0 HOURS")

print("Vacuum completed on sat_order with 0-hour retention.")
