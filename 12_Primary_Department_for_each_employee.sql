-- Question (https://leetcode.com/problems/primary-department-for-each-employee/?envType=study-plan-v2&envId=top-sql-50)

SQL Code - 
select  employee_id , department_id 
 from Employee 
  where primary_flag = 'Y'
  or employee_id in (
    select employee_id 
    from Employee
    group by employee_id 
    having count(*)=1
  )

Pyspark Code - 
from pyspark.sql import functions as F

# Assuming Employee is already a Spark DataFrame
employee_df = Employee

# Subquery: employee_ids that appear only once
single_employee_df = (
    employee_df
    .groupBy("employee_id")
    .count()
    .filter(F.col("count") == 1)
    .select("employee_id")
)

# Main query logic
result_df = (
    employee_df
    .filter(
        (F.col("primary_flag") == "Y") |
        (F.col("employee_id").isin(
            [row.employee_id for row in single_employee_df.collect()]
        ))
    )
    .select("employee_id", "department_id")
)

result_df.show()