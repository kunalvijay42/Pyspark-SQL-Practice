-- Question (https://leetcode.com/problems/find-total-time-spent-by-each-employee/)

SQL Code -
WITH CTE AS (
    Select *, out_time - in_time as total_time 
    from Employees e
)

Select event_day as day, emp_id, sum(total_time) as total_time 
from CTE
group by 1,2;

Pyspark Code -
WITH CTE AS (
    Select *, out_time - in_time as total_time 
    from Employees e
)

Select event_day as day, emp_id, sum(total_time) as total_time 
from CTE
group by 1,2;