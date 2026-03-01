-- Question (https://leetcode.com/problems/number-of-unique-subjects-taught-by-each-teacher/?envType=study-plan-v2&envId=top-sql-50)

SQL Code -
select teacher_id, Count(distinct subject_id) as cnt from Teacher group by teacher_id


Pyspark Code -

from pyspark.sql import functions as F
result_df = (
    teacher_df
    .groupBy("teacher_id")
    .agg(F.countDistinct("subject_id").alias("cnt"))
    .select("teacher_id", "cnt")
)

result_df.show()