-- Question (https://leetcode.com/problems/project-employees-i/?envType=study-plan-v2&envId=top-sql-50)

SQL Code - 
With CTE AS (
    Select p.project_id, p.employee_id, e.experience_years from Project p inner join Employee e on p.employee_id=e.employee_id
)

Select project_id, ROUND(AVG(experience_years),2) as average_years from CTE group by project_id

Pyspark Code -
from pyspark.sql import functions as F

# Step 1: Join (equivalent to CTE)
cte_df = (
    project_df.alias("p")
    .join(
        employee_df.alias("e"),
        F.col("p.employee_id") == F.col("e.employee_id"),
        "inner"
    )
    .select(
        F.col("p.project_id"),
        F.col("p.employee_id"),
        F.col("e.experience_years")
    )
)

# Step 2: Group By + Average + Round
result_df = (
    cte_df
    .groupBy("project_id")
    .agg(
        F.round(F.avg("experience_years"), 2).alias("average_years")
    )
)

result_df.show()