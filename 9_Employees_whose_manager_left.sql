-- Question (https://leetcode.com/problems/employees-whose-manager-left-the-company/submissions/1825248287/?envType=study-plan-v2&envId=top-sql-50)

SQL Code - 
select employee_id 
from Employees e
where e.salary<30000 and e.manager_id not in (select employee_id from Employees)
order by e.employee_id;