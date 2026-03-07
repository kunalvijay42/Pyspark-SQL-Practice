-- Question (https://leetcode.com/problems/confirmation-rate/?envType=study-plan-v2&envId=top-sql-50)

SQL Code - 
select s.user_id,
  ROUND(
    CASE
      WHEN COUNT(c.action) = 0 THEN 0
      ELSE SUM(c.action = 'confirmed') / COUNT(c.action)
    END
  , 2) AS confirmation_rate   
from 
Signups s LEFT JOIN
Confirmations c ON s.user_id=c.user_id
group by s.user_id;

PySpark Code -
from pyspark.sql import functions as F

result_df = (
    signups_df.alias("s")
    .join(confirmations_df.alias("c"), F.col("s.user_id") == F.col("c.user_id"), "left")
    .groupBy("s.user_id")
    .agg(
        F.round(
            F.when(
                F.count("c.action") == 0, 
                F.lit(0)
            ).otherwise(
                F.sum(F.when(F.col("c.action") == "confirmed", 1).otherwise(0)) / F.count("c.action")
            ),
            2
        ).alias("confirmation_rate")
    )
)

result_df.show()