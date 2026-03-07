-- Question (https://leetcode.com/problems/exchange-seats/?envType=study-plan-v2&envId=top-sql-50)

SQL Code - 
SELECT
    CASE
       WHEN id%2=1 AND id=(SELECT MAX(id) FROM Seat) THEN id
       WHEN id%2=0 THEN id-1
       ELSE id + 1
    END AS id,
    student
FROM Seat
ORDER BY id;

Pyspark Code -
from pyspark.sql import functions as F

# Get max id
max_id = df.agg(F.max("id")).collect()[0][0]

result_df = df.withColumn(
    "new_id",
    F.when((F.col("id") % 2 == 1) & (F.col("id") == max_id), F.col("id"))
     .when(F.col("id") % 2 == 0, F.col("id") - 1)
     .otherwise(F.col("id") + 1)
).select(
    F.col("new_id").alias("id"),
    "student"
).orderBy("id")

result_df.show()