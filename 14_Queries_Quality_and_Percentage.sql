-- Question (https://leetcode.com/problems/queries-quality-and-percentage/?envType=study-plan-v2&envId=top-sql-50)

SQL Code -  

select query_name,
round(sum(rating/position)/count(query_name),2) as quality,
round(SUM(CASE WHEN rating < 3 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as poor_query_percentage
from Queries
group by 1;

Pyspark Code - 

from pyspark.sql import functions as F

result_df = (
    Queries
    .groupBy("query_name")
    .agg(
        F.round(
            F.sum(F.col("rating") / F.col("position")) / F.count("query_name"),
            2
        ).alias("quality"),
        
        F.round(
            F.sum(
                F.when(F.col("rating") < 3, 1).otherwise(0)
            ) * 100.0 / F.count("*"),
            2
        ).alias("poor_query_percentage")
    )
)

result_df.show()