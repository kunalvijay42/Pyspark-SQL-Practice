-- Question (https://leetcode.com/problems/rising-temperature/submissions/1818377984/?envType=study-plan-v2&envId=top-sql-50)

Table: Weather

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| recordDate    | date    |
| temperature   | int     |
+---------------+---------+
id is the column with unique values for this table.
There are no different rows with the same recordDate.
This table contains information about the temperature on a certain day.
 

Write a solution to find all dates' id with higher temperatures compared to its previous dates (yesterday).

Return the result table in any order.

SQL Code - 
select w1.id
from Weather w1
JOIN Weather w2
where datediff(w1.recordDate, w2.recordDate)=1 and w1.temperature>w2.temperature;

Pyspqark Code - 

from pyspark.sql import functions as F

w1 = weather_df.alias("w1")
w2 = weather_df.alias("w2")

# Self-join
result_df = (
    w1.join(w2, how="inner")  # inner join, SQL JOIN defaults to inner if no ON in Spark
      .filter(F.datediff(F.col("w1.recordDate"), F.col("w2.recordDate")) == 1)
      .filter(F.col("w1.temperature") > F.col("w2.temperature"))
      .select(F.col("w1.id"))
)

result_df.show()