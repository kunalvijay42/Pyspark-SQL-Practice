-- Question (https://leetcode.com/problems/odd-and-even-transactions/)

SQL Code - 
select transaction_date,
SUM(CASE WHEN amount%2=1 Then amount else 0 END) as odd_sum,
SUM(CASE WHEN amount%2=0 Then amount else 0 END) as even_sum
from transactions
group by 1
order by 1;

Pyspark Code - 
from pyspark.sql import functions as F

result = (
    transactions
    .groupBy("transaction_date")
    .agg(
        F.sum(F.when(F.col("amount") % 2 == 1, F.col("amount")).otherwise(0)).alias("odd_sum"),
        F.sum(F.when(F.col("amount") % 2 == 0, F.col("amount")).otherwise(0)).alias("even_sum")
    )
    .orderBy("transaction_date")
)

result.show()