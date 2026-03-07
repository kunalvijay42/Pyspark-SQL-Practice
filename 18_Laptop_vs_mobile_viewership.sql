-- Question (https://datalemur.com/questions/laptop-mobile-viewership)

SQL Code -
SELECT 
  SUM(CASE WHEN device_type = 'laptop' THEN 1 ELSE 0 END) AS laptop_views, 
  SUM(CASE WHEN device_type IN ('tablet', 'phone') THEN 1 ELSE 0 END) AS mobile_views 
FROM viewership;

Pyspark Code -
from pyspark.sql import functions as F

# Aggregate laptop and mobile views
result = viewership.agg(
    F.sum(F.when(F.col("device_type") == "laptop", 1).otherwise(0)).alias("laptop_views"),
    F.sum(F.when(F.col("device_type").isin("tablet", "phone"), 1).otherwise(0)).alias("mobile_views")
).select(
    F.col("laptop_views"),
    F.col("mobile_views")
)

result.show()