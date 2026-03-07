-- Question (https://datalemur.com/questions/sql-histogram-tweets)

SQL Code -
with cte as (
select user_id, count(tweet_id) as cnt  
from tweets
where EXTRACT(year from tweet_date) = '2022'
group by user_id )

select cnt as tweet_bucket, count(user_id) as users_num
from cte
group by cnt

Pyspark Code -
from pyspark.sql import functions as F

# Filter tweets from 2022
cte = (
    tweets
    .filter(F.year("tweet_date") == 2022)
    .groupBy("user_id")
    .agg(F.count("tweet_id").alias("cnt"))
)

# Final aggregation
result = (
    cte
    .groupBy("cnt")
    .agg(F.count("user_id").alias("users_num"))
    .withColumnRenamed("cnt", "tweet_bucket")
)

result.show()