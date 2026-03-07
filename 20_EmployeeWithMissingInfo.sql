-- Question (https://leetcode.com/problems/employees-with-missing-information/)

SQL Code -
select e.employee_id from employees e  left join salaries s on e.employee_id=s.employee_id where s.salary is null
union 
select s.employee_id from salaries s  left join employees e on e.employee_id=s.employee_id where e.name is null
order by employee_id;

Pyspark Code - 
from pyspark.sql import functions as F

# Employees without salaries
emp_without_salary = (
    employees.alias("e")
    .join(salaries.alias("s"), F.col("e.employee_id") == F.col("s.employee_id"), "left")
    .filter(F.col("s.salary").isNull())
    .select(F.col("e.employee_id"))
)

# Salaries without employees
salary_without_emp = (
    salaries.alias("s")
    .join(employees.alias("e"), F.col("s.employee_id") == F.col("e.employee_id"), "left")
    .filter(F.col("e.name").isNull())
    .select(F.col("s.employee_id"))
)

# Union and order
result = (
    emp_without_salary
    .union(salary_without_emp)
    .orderBy("employee_id")
)

result.show()