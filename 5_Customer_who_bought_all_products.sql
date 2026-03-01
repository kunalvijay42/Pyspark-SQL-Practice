-- Question (https://leetcode.com/problems/customers-who-bought-all-products/description/?envType=study-plan-v2&envId=top-sql-50)

SQL Code - 
SELECT CUSTOMER_ID FROM CUSTOMER GROUP BY CUSTOMER_ID HAVING COUNT(DISTINCT PRODUCT_KEY) = (SELECT COUNT(*) FROM PRODUCT);

Pyspark Code - 
from pyspark.sql import functions as F

# Get total product count
total_products = df_product.count()

# Find customers who purchased all products
result = df_customer \
    .groupBy("CUSTOMER_ID") \
    .agg(F.countDistinct("PRODUCT_KEY").alias("distinct_products")) \
    .filter(F.col("distinct_products") == total_products) \
    .select("CUSTOMER_ID")

result.show()