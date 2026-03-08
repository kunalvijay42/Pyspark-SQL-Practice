-- Question (https://leetcode.com/problems/market-analysis-i/submissions/1896589460/)

SQL Code - 
SELECT 
    users.user_id as buyer_id,
    users.join_date,
    COALESCE(COUNT(orders.buyer_id),0) as orders_in_2019
FROM users
LEFT JOIN orders ON orders.buyer_id = users.user_id
AND
    EXTRACT(YEAR FROM orders.order_date) = 2019
GROUP BY
    users.user_id, users.join_date

Pyspark Code -
from pyspark.sql import functions as F

result_df = (
    users_df.alias("u")
    .join(
        orders_df.alias("o"),
        (F.col("u.user_id") == F.col("o.buyer_id")) &
        (F.year(F.col("o.order_date")) == 2019),
        "left"
    )
    .groupBy("u.user_id", "u.join_date")
    .agg(
        F.count("o.buyer_id").alias("orders_in_2019")
    )
    .select(
        F.col("u.user_id").alias("buyer_id"),
        F.col("u.join_date"),
        F.col("orders_in_2019")
    )
)

result_df.show()