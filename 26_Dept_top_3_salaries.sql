-- Question (https://leetcode.com/problems/department-top-three-salaries/)

Sql Code - 
WITH CTE AS (
    Select d.name as Department, e.name as Employee, e.salary as Salary, DENSE_RANK() OVER (PARTITION BY d.name ORDER BY e.salary DESC) as rn from Employee e INNER JOIN Department d on e.departmentId=d.id
)

select Department, Employee, Salary from CTE where rn<=3; 

Pyspark Code -
from pyspark.sql import functions as F
from pyspark.sql.window import Window

joined = employee.join(department, employee.departmentId == department.id)

window_spec = Window.partitionBy("Department").orderBy(F.col("Salary").desc())

result = (
    joined
    .select(
        department.name.alias("Department"),
        employee.name.alias("Employee"),
        employee.salary.alias("Salary")
    )
    .withColumn("rn", F.dense_rank().over(window_spec))
    .filter(F.col("rn") <= 3)
    .select("Department", "Employee", "Salary")
)

result.show()