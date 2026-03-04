-- Question (https://leetcode.com/problems/count-salary-categories/description/?envType=study-plan-v2&envId=top-sql-50)

SQL Code - 
SELECT 
    'Low Salary' AS category,
    COUNT(account_id) AS accounts_count
FROM Accounts 
WHERE income < 20000

UNION

SELECT 
    'Average Salary' AS category,
    COUNT(account_id) AS accounts_count
FROM Accounts 
WHERE income BETWEEN 20000 AND 50000

UNION

SELECT 
    'High Salary' AS category,
    COUNT(account_id)  AS accounts_count
FROM Accounts
WHERE income > 50000

Pyspark Code - 

from pyspark.sql import functions as F

accounts_df = Accounts

# Low Salary
low_salary_df = (
    accounts_df
    .filter(F.col("income") < 20000)
    .agg(F.count("account_id").alias("accounts_count"))
    .withColumn("category", F.lit("Low Salary"))
    .select("category", "accounts_count")
)

# Average Salary
avg_salary_df = (
    accounts_df
    .filter(F.col("income").between(20000, 50000))
    .agg(F.count("account_id").alias("accounts_count"))
    .withColumn("category", F.lit("Average Salary"))
    .select("category", "accounts_count")
)

# High Salary
high_salary_df = (
    accounts_df
    .filter(F.col("income") > 50000)
    .agg(F.count("account_id").alias("accounts_count"))
    .withColumn("category", F.lit("High Salary"))
    .select("category", "accounts_count")
)

# Union all results
result_df = low_salary_df.union(avg_salary_df).union(high_salary_df)

result_df.show()