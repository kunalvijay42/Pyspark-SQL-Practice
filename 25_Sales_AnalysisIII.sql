-- Question (https://leetcode.com/problems/sales-analysis-iii/submissions/1942234517/)

SQL Code - 
SELECT p.product_id, p.product_name
FROM Sales s
JOIN Product p ON s.product_id = p.product_id
GROUP BY p.product_id, p.product_name
HAVING MIN(s.sale_date) >= '2019-01-01' 
   AND MAX(s.sale_date) <= '2019-03-31'

Pyspark Code - 
from pyspark.sql import functions as F

result_df = (
    sales_df.alias("s")
    .join(product_df.alias("p"), F.col("s.product_id") == F.col("p.product_id"), "inner")
    .groupBy("p.product_id", "p.product_name")
    .agg(
        F.min("s.sale_date").alias("min_sale_date"),
        F.max("s.sale_date").alias("max_sale_date")
    )
    .filter(
        (F.col("min_sale_date") >= "2019-01-01") &
        (F.col("max_sale_date") <= "2019-03-31")
    )
    .select("product_id", "product_name")
)

result_df.show()