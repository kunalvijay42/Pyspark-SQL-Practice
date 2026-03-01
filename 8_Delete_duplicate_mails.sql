-- Question (https://leetcode.com/problems/delete-duplicate-emails/description/?envType=study-plan-v2&envId=top-sql-50)

SQL Code 

DELETE FROM Person
WHERE id NOT IN (
  SELECT MIN(id)
  FROM Person
  GROUP BY email
);