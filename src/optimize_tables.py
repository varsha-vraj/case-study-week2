from config import get_spark_session

spark = get_spark_session()

# Full absolute paths
sat_order_path = "delta.`file:///C:/Users/varsha.v/Desktop/ecommerce-data-vault/delta_tables/sat_order`"
sat_product_path = "delta.`file:///C:/Users/varsha.v/Desktop/ecommerce-data-vault/delta_tables/sat_product`"
link_cust_order_path = "delta.`file:///C:/Users/varsha.v/Desktop/ecommerce-data-vault/delta_tables/link_customer_order`"

# Optimize
print("Optimizing sat_order on order_date...")
spark.sql(f"OPTIMIZE {sat_order_path} ZORDER BY (order_date)")

print("Optimizing sat_product on Product_HK...")
spark.sql(f"OPTIMIZE {sat_product_path} ZORDER BY (Product_HK)")

print("Optimizing link_customer_order on Customer_HK, Order_HK...")
spark.sql(f"OPTIMIZE {link_cust_order_path} ZORDER BY (Customer_HK, Order_HK)")

