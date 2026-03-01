-- Question (https://leetcode.com/problems/customer-who-visited-but-did-not-make-any-transactions/?envType=study-plan-v2&envId=top-sql-50)
Table: Visits

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| visit_id    | int     |
| customer_id | int     |
+-------------+---------+
visit_id is the column with unique values for this table.
This table contains information about the customers who visited the mall.
 

Table: Transactions

+----------------+---------+
| Column Name    | Type    |
+----------------+---------+
| transaction_id | int     |
| visit_id       | int     |
| amount         | int     |
+----------------+---------+
transaction_id is column with unique values for this table.
This table contains information about the transactions made during the visit_id.
 

Write a solution to find the IDs of the users who visited without making any transactions and the number of times they made these types of visits.

Return the result table sorted in any order.

SQL Solution - 

Select v.customer_id, count(*) as count_no_trans    
from Visits v
LEFT JOIN
Transactions t
on 
v.visit_id=t.visit_id
where t.transaction_id IS NULL
GROUP BY v.customer_id;

Pyspark Code - 

from pyspark.sql import functions as F

result_df = (
    visits_df.alias("v")
    .join(
        transactions_df.alias("t"),
        F.col("v.visit_id") == F.col("t.visit_id"),
        "left"
    )
    .filter(F.col("t.transaction_id").isNull())
    .groupBy("v.customer_id")
    .agg(F.count("*").alias("count_no_trans"))
)

result_df.show()