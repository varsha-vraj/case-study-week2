from config import get_spark_session

spark = get_spark_session()


base = "file:///C:/Users/varsha.v/Desktop/ecommerce-data-vault/delta_tables"


# PIT Customer 
df_cust = spark.read.format("delta").load(f"{base}/scd_sat_customer")
pit_cust = df_cust.filter("Is_Current = true") \
                  .select("Customer_HK", "Start_Date") \
                  .withColumnRenamed("Start_Date", "Latest_Sat_Load_Date")
pit_cust.write.format("delta").mode("overwrite").save(f"{base}/pit_customer")
print(" PIT_Customer created")

#  PIT Product 
df_prod = spark.read.format("delta").load(f"{base}/scd_sat_product")
pit_prod = df_prod.filter("Is_Current = true") \
                  .select("Product_HK", "Start_Date") \
                  .withColumnRenamed("Start_Date", "Latest_Sat_Load_Date")
pit_prod.write.format("delta").mode("overwrite").save(f"{base}/pit_product")
print(" PIT_Product created")

# PIT Order 
df_order = spark.read.format("delta").load(f"{base}/scd_sat_order")
pit_order = df_order.filter("Is_Current = true") \
                    .select("Order_HK", "Start_Date") \
                    .withColumnRenamed("Start_Date", "Latest_Sat_Load_Date")
pit_order.write.format("delta").mode("overwrite").save(f"{base}/pit_order")
print(" PIT_Order created")
