-- Question (https://leetcode.com/problems/percentage-of-users-attended-a-contest/)

SQL Code - 
select contest_id,
ROUND((COUNT(distinct user_id)*100)/(Select count(*) from Users),2) as percentage
from Register 
group by contest_id
Order by percentage DESC, contest_id ASC


Pyspark Code 
total_users = users_df.count()

result_df = (
    register_df
    .groupBy("contest_id")
    .agg(
        F.round(
            (F.countDistinct("user_id") * 100.0) / total_users,
            2
        ).alias("percentage")
    )
    .orderBy(
        F.col("percentage").desc(),
        F.col("contest_id").asc()
    )
)

result_df.show()