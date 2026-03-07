-- Question (https://datalemur.com/questions/duplicate-job-listings)

SQL Code - 
WITH CTE AS

(SELECT company_id, title, description, count(*)
FROM job_listings
GROUP BY company_id, title, description
HAVING count(*) > 1)

SELECT count(*) FROM CTE

Pyspark Code -
from pyspark.sql import functions as F

# Group by the columns and count
cte_df = (
    job_listings
    .groupBy("company_id", "title", "description")
    .agg(F.count("*").alias("count"))
    .filter(F.col("count") > 1)
)

# Count the number of duplicate groups
result = cte_df.count()

print(result)