-- Question (https://leetcode.com/problems/list-the-products-ordered-in-a-period/?envType=study-plan-v2&envId=top-sql-50)


SQL Code - 
Select p.product_name , sum(unit) as unit from Products p 
join orders o 
on p.product_id = o.product_id 
where order_date  between '2020-02-01' and  '2020-02-29'
group by p.product_id
having unit >=100;


Pyspark Code - 
from pyspark.sql import functions as F

result_df = (
    products_df.alias("p")
    .join(
        orders_df.alias("o"),
        F.col("p.product_id") == F.col("o.product_id"),
        "inner"
    )
    .filter(
        (F.col("o.order_date") >= "2020-02-01") &
        (F.col("o.order_date") <= "2020-02-29")
    )
    .groupBy("p.product_id", "p.product_name")
    .agg(
        F.sum("o.unit").alias("unit")
    )
    .filter(F.col("unit") >= 100)
    .select("product_name", "unit")
)

result_df.show()