-- Question (https://leetcode.com/problems/sales-person/)

SQL Code - 
select s.name from  SalesPerson s
where s.sales_id not in (select sales_id from Orders o inner join Company c
    on o.com_id=c.com_id where c.name='RED')

Pyspark Code -
red_sales = (
    orders_df
    .join(company_df, "com_id")
    .filter(F.col("name") == "RED")
    .select("sales_id")
)

result_df = sales_df.join(red_sales, "sales_id", "left_anti").select("name")