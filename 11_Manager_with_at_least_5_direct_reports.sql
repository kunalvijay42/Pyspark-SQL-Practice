-- Question (https://leetcode.com/problems/managers-with-at-least-5-direct-reports/?envType=study-plan-v2&envId=top-sql-50)

SQL Code -
select name
from Employee
where id IN (
    Select managerId from 
    Employee
    group by managerId
    having count(managerId)>=5
)

Pyspark Code - 

from pyspark.sql import functions as F

managers_df = (
    employee_df
    .groupBy("managerId")
    .agg(F.count("managerId").alias("report_count"))
    .filter(F.col("report_count") >= 5)
    .select(F.col("managerId").alias("id"))
)

result_df = (
    employee_df
    .join(managers_df, on="id", how="inner")
    .select("name")
)

result_df.show()