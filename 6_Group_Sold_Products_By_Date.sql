-- Question (https://leetcode.com/problems/group-sold-products-by-the-date/?envType=study-plan-v2&envId=top-sql-50)


SQL Code - 

select sell_date, count(distinct product) as num_sold , 
group_concat(distinct product order by product asc separator ',') as products               
from activities  
group by sell_date
order by sell_date


Pyspark Code -

from pyspark.sql import functions as F

result_df = (
    activities_df
    .groupBy("sell_date")
    .agg(
        F.countDistinct("product").alias("num_sold"),
        F.concat_ws(
            ",",
            F.sort_array(
                F.collect_set("product")
            )
        ).alias("products")
    )
    .orderBy("sell_date")
)

result_df.show(truncate=False)