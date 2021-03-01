# POC Spark Problem Solving

# Part 1

## Employee dataset
In HDFS/data/spark/employee, there are 4 files:
- dept.txt
- dept-with-header.txt
- emp.txt
- emp-with-header.txt

give employee and dept. information.
![99](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/employee_dataset.png)

Answer following questions by:

(1) Spark SQL

(2) Spark DataFrame API


## Question 1
```
List total salary for each dept.
```

## Solution 1
- Create Schemas
```
import org.apache.spark.sql.types._

val dept_schema=new StructType(Array(
new StructField("DEPT_NO",LongType,false),
new StructField("DEPT_NAME",StringType,true),
new StructField("LOC",StringType,true),
))

val emp_schema=new StructType(Array(
new StructField("EMPNO",LongType,false),
new StructField("NAME",StringType,true),
new StructField("JOB",StringType,true),
new StructField("MGR",LongType,false),
new StructField("HIREDATE",DateType,true),
new StructField("SAL",LongType,false),
new StructField("COMM",LongType,false),
new StructField("DEPTNO",LongType,false)
))
```
- Spark read text file into DataFrame by specify schmema
```
val path1="/home/pan/Documents/tutorial/data/dept-with-header.txt"
val dept_with_header_df=spark.read.format("csv").option("header","true").option("mode","FAILFAST").schema(dept_schema).load(path1)
dept_with_header_df.show(truncate=false)

val path2="/home/pan/Documents/tutorial/data/emp-with-header.txt"
val emp_with_header_df=spark.read.option("header","true").option("mode","FAILFAST").schema(emp_schema).csv(path2)
emp_with_header_df.show(truncate=false)
```
- Spark read text file into DataFrame by infer schema
```
val path1="/data/spark/employee/dept-with-header.txt"
val dept_with_header_df=spark.read.format("csv").option("header","true").option("inferSchema",true).option("sep",",").option("mode","FAILFAST").load(path1)
dept_with_header_df.show(truncate=false)
dept_with_header_df.printSchema()

val path2="/data/spark/employee/emp-with-header.txt"
val emp_with_header_df=spark.read.format("csv").option("header","true").option("inferSchema",true).option("sep",",").option("mode","FAILFAST").load(path2)
emp_with_header_df.show(truncate=false)
emp_with_header_df.printSchema()
```


- Inner joins
```
val joinExpression=dept_with_header_df.col("DEPT_NO")===emp_with_header_df.col("DEPTNO")
val employee_df=emp_with_header_df.join(dept_with_header_df,joinExpression).drop(emp_with_header_df.col("DEPTNO"))
employee_df.show(false)
```
- Create a Temp View from a dataframe
```
employee_df.createOrReplaceTempView("employee_table")
spark.catalog.listTables.show(false)
spark.sql("select * from employee_table").show
```
![98](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/view.png)


Method 1 - Spark DataFrame API
```
employee_df.groupBy("DEPT_NO","DEPT_NAME").sum("SAL").show(truncate=false)
```


Method 2 - Spark SQL
```
spark.sql("select DEPT_NAME,sum(SAL) as total_salary from employee_table group by DEPT_NAME").show(false)
```
![1](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q1.png)







## Question 2
```
List total number of employee and average salary for each dept.
```

## Solution 2
Method 1 - Spark DataFrame API
```
employee_df.count()
import org.apache.spark.sql.functions.{avg}
employee_df.groupBy("DEPT_NAME").avg("SAL").show()
```
![2](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q2.png)
```
import org.apache.spark.sql.functions._
val Q2_1 = emp_with_header_df.groupBy("DEPTNO").agg(avg("SAL"),count("*").as("number"))
Q2_1.show
```
![199](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/199.png)
```
val join_expr=dept_with_header_df.col("DEPT_NO")===emp_with_header_df.col("DEPTNO")
val join_df=emp_with_header_df.join(dept_with_header_df,join_expr)
join_df.show(false)
val Q2_2=join_df.groupBy("DEPTNO","DEPT_NAME").agg(avg("SAL"),count("*").as("number"))
Q2_2.show
```
![198](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/198.png)

Method 2 - Spark SQL
```
spark.sql("select count(*) from employee_table").show(false)
spark.sql("select DEPT_NAME,avg(SAL) as avg_salary from employee_table group by DEPT_NAME").show(false)
```
```
val Q2_3=spark.sql("""
select DEPTNO, DEPT_NAME, avg(SAL), count(*) as number
from employee_table
group by DEPTNO, DEPT_NAME
""")
Q2_3.show
```



## Question 3
```
List the first hired employee's name for each dept.
```

## Solution 3
Method 1 - Spark DataFrame API
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val w1=Window.partitionBy("DEPT_NAME").orderBy(col("HIREDATE"))
employee_df.withColumn("row",row_number.over(w1)).where($"row"===1).drop("row").select("NAME","DEPT_NAME").show(false)
```
or
```
val Q3_1=emp_with_header_df.groupBy(col("DEPTNO").as("DEPTNO2")).agg(min("HIREDATE").as("min_hiredate"))
Q3_1.show(false)
val join_expr1= (Q3_1.col("DEPTNO2")===emp_with_header_df.col("DEPTNO")) && (Q3_1.col("min_hiredate")===emp_with_header_df.col("HIREDATE"))
val Q3_2=emp_with_header_df.join(Q3_1,join_expr1).select("DEPTNO2","NAME","HIREDATE")
Q3_2.show
```





Method 2 - Spark SQL
```
spark.sql("select s.NAME, s.HIREDATE, s.DEPT_NAME from (select employee_table.NAME, employee_table.HIREDATE, employee_table.DEPT_NAME, ROW_NUMBER() OVER(PARTITION BY employee_table.DEPT_NAME ORDER BY employee_table.HIREDATE) as rk from employee_table) as s where s.rk=1").show(false)
```
or
```
emp_with_header_df.createOrReplaceTempView("emp_table")
val Q3_3=spark.sql("""
select a.DEPTNO, e.NAME, e.hiredate
from 
(
select DEPTNO, min(HIREDATE) as min_hiredate
from emp_table
group by DEPTNO
) as a
join emp_table e
on a.DEPTNO= e.DEPTNO and a.min_hiredate=e.HIREDATE
""")
Q3_3.show(false)
```
![3](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q3.png)



## Question 4
```
List total employee salary for each city.
```


## Solution 4
Method 1 - Spark DataFrame API
```
employee_df.groupBy("LOC").sum("SAL").show(false)
```
or
```
val Q4_1=emp_with_header_df.groupBy("DEPTNO").agg(sum("SAL").as("total_salary"))
val join_expr=(Q4_1.col("DEPTNO")===dept_with_header_df.col("DEPT_NO"))
val Q4_2=Q4_1.join(dept_with_header_df,join_expr)
val Q4_3=Q4_2.select("total_salary","LOC")
Q4_3.show(false)
```



Method 2 - Spark SQL
```
spark.sql("select sum(SAL), LOC from employee_table group by LOC").show(false)
```
or
```
emp_with_header_df.createOrReplaceTempView("emp_table")
dept_with_header_df.createOrReplaceTempView("dept_table")
val Q4_4=spark.sql("""
select d.LOC, sum(e.SAL) as total_salary
from emp_table e
join dept_table d
on e.DEPTNO=d.DEPT_NO
group by d.LOC
""")
Q4_4.show(false)
```
![4](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q4.png)





## Question 5
```
List employee's name and salary whose salary is higher than their manager.
```


## Solution 5
Method 1 - Spark DataFrame API
```
val employee2_df=employee_df.withColumnRenamed("SAL","MGR_SAL").withColumnRenamed("NAME","MGR_NAME")
val employee_selfdf=employee_df.as("emp1").join(employee2_df.as("emp2"),col("emp1.MGR") === col("emp2.EMPNO"),"inner").drop(col("emp2.EMPNO"))
employee_selfdf.select("NAME","SAL").where(col("MGR_SAL")<col("SAL")).show
```
or
```
val Q5_1=emp_with_header_df.select(col("EMPNO").as("emp_no"),col("SAL").as("MGR_SAL"))
Q5_1.show(false)
val join_expr=(Q5_1.col("emp_no")===emp_with_header_df.col("MGR"))
val Q5_2=emp_with_header_df.join(Q5_1, join_expr)
Q5_2.show(false)
val Q5_3=Q5_2.where(col("SAL")>col("MGR_SAL"))
Q5_3.show(false)
val Q5_4=Q5_3.select("NAME","SAL")
Q5_4.show(false)
```
or
```
val df_q5_1=emp_with_header_df.select(col("EMPNO").as("emp_no"),col("SAL").as("salary"))
val join_expr_5 = (emp_with_header_df.col("MGR") === df_q5_1.col("emp_no")) && (emp_with_header_df.col("SAL") > df_q5_1.col("salary"))
val df_q5_2 = emp_with_header_df.join(df_q5_1, join_expr_5).select("NAME", "SAL")
df_q5_2.show
```

Method 2 - Spark SQL
```
employee_selfdf.createOrReplaceTempView("employee_selftable")
spark.catalog.listTables.show(false)
spark.sql("select * from employee_selftable").show
spark.sql("select NAME, SAL from employee_selftable where MGR_SAL<SAL").show(false)
```
or
```
spark.sql("""
select e1.NAME, e1.SAL
from employee_table e1 INNER JOIN employee_table e2 on e1.MGR==e2.EMPNO
where e1.SAL>e2.SAL
""").show(false)
```
or
```
emp_with_header_df.createOrReplaceTempView("emp_table")
val Q5_5=spark.sql("""
select e1.NAME, e1.SAL
from emp_table e1
join emp_table e2
on e1.MGR=e2.EMPNO
where e1.SAL>e2.SAL
""")
Q5_5.show(false)
```
![5](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q5.png)





## Question 6
```
List employee's name and salary whose salary is higher than average salary of whole company.
```


## Solution 6
Method 1 - Spark DataFrame API
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val AVG_SAL=employee_df.select(avg("SAL")).collect()(0).getDouble(0)//cal the avg value
val employee_avg_df =employee_df.withColumn("AVG_SAL", rand() * 0 +AVG_SAL)//when adding a new column, set the original value as 0, and then add the avg value
employee_avg_df.select("NAME","SAL").where(col("AVG_SAL")<col("SAL")).show
```
or
```
val w1=Window.partitionBy()
val employee_avg_df=employee_df.withColumn("AVG_SAL", avg(col("SAL")).over(w1)).select("NAME","SAL").where(col("AVG_SAL")<col("SAL"))
employee_avg_df.show(false)
```
or
```
val Q6_1=emp_with_header_df.select(avg("SAL"))
val avg_sal=Q6_1.first().getDouble(0)
val Q6_2=emp_with_header_df.select("NAME","SAL").where(col("SAL")>lit(avg_sal))
Q6_2.show
```





Method 2 - Spark SQL
```
spark.sql("select AVG(SAL) from employee_table").show(false)
spark.sql("""
select NAME, SAL
from employee_table
where SAL>2077.0833333333335
""").show(false)
```
or
```
spark.sql("""
select NAME, SAL
from employee_table
where SAL>(select AVG(SAL) from employee_table)
""").show(false)
```
or
```
spark.sql("""
select NAME, SAL
from(
select NAME, SAL, AVG(SAL) OVER () AS AVG_SAL
from employee_table
)
where SAL > AVG_SAL
""").show(false)
```
or
```
emp_with_header_df.createOrReplaceTempView("emp_table")
val Q6=spark.sql("""
select name, sal
from emp_table e1
join (select avg(sal) as avg_sal from emp_table) e2
on e1.sal>e2.avg_sal
""")
Q6.show(false)
```
![6](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q6.png)








## Question 7
```
List employee's name and dept name whose name start with "J".
```


## Solution 7
Method 1 - Spark DataFrame API
```
employee_df.filter(col("NAME").startsWith("J")).select("NAME","DEPT_NAME").show(false)
```
or
```
val Q7_1=emp_with_header_df.where("NAME LIKE 'J%'")
val join_expr = (Q7_1.col("DEPTNO")===dept_with_header_df.col("DEPT_NO"))
val Q7_2=Q7_1.join(dept_with_header_df,join_expr).select("NAME","DEPT_NAME")
Q7_2.show
```



Method 2 - Spark SQL
```
spark.sql("""
select NAME,DEPT_NAME
from employee_table
where NAME LIKE "J%"
""").show(false)
```
or
```
emp_with_header_df.createOrReplaceTempView("emp_table")
dept_with_header_df.createOrReplaceTempView("dept_table")
val Q7=spark.sql("""
select e.NAME, d.DEPT_NAME
from emp_table e
join dept_table d
on e.DEPTNO=d.DEPT_NO
where e.name like "J%"
""")
Q7.show
```
![7](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q7.png)





## Question 8
```
List 3 employee's name and salary with highest salary.
```


## Solution 8
Method 1 - Spark DataFrame API
```
employee_df.orderBy(col("SAL").desc).select("NAME","SAL").take(3).foreach(println)
```
or
```
val Q8=emp_with_header_df.orderBy(desc("SAL")).select("NAME","SAL").limit(3)
Q8.show
```


Method 2 - Spark SQL
```
spark.sql("""
select NAME, SAL
from employee_table
order by SAL DESC
limit 3
""").show(false)
```
![8](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q8.png)



## Question 9
```
Sort employee by total income (salary+commission), list name and total income.
```


## Solution 9
Method 1 - Spark DataFrame API
```
employee_df.withColumn("totalincome",col("SAL")+col("COMM")).sort(desc("totalincome")).select("NAME","totalincome").show(false)
```
or
```
val Q9=emp_with_header_df.select(col("NAME"),(col("SAL")+col("COMM")).as("total_income")).sort(desc("total_income"))
Q9.show
```

Method 2 - Spark SQL
```
spark.sql("""
select NAME, SAL+COMM as totalincome
from employee_table
order by totalincome DESC
""").show(false)
```
![9](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/Q9.png)


# Part 2

## Refer to week 03 hive homework, implement all queries by Spark DataFrame API
```
ssh pan@54.86.193.122 
ssh pan@ip-172-31-92-98.ec2.internal
spark-shell
```
## Question 1

Write queries on banklist table:

```
Find top 5 states with most banks. The result must show the state name and number of banks in descending order.
Found how many banks were closed each year. The result must show the year and the number of banks closed on that year, order by year.
```

## Solution 1
- Create DataFrame from table
```
val banklist_df=spark.read.table("roger_db.banklist").where("bankname != 'Bank Name'")
banklist_df.printSchema
banklist_df.show(truncate=false)
```


- Find top 5 states with most banks. The result must show the state name and number of banks in descending order.
```
val banklist_most_df=banklist_df.select("state").groupBy("state").count().orderBy(desc("count")).limit(5)
banklist_most_df.show
```
![1](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q1_1.png)


- Found how many banks were closed each year. The result must show the year and the number of banks closed on that year, order by year.
```
import org.apache.spark.sql.functions._
val banklist_close_df=banklist_df.select(col("Closing Date"),substring(col("Closing Date"),-2,2).alias("yr")).groupBy("yr").count().orderBy(desc("yr"))
banklist_close_df.show
```
![2](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q1_2.png)







## Question 2
`https://data.cityofchicago.org/Public-Safety/Crimes-2020/qzdf-xmn8`

Check hive database named “chicago”, there is one table named: 
crime_parquet: include data from 2001-present
```
1. In your own database, create a partitioned table (partitionedby year) to store data, store in parquet format. Name the table “crime_parquet_16_20”;
2. Import 2016 to 2020 data into the partitioned table from table chicago.crime_parquet
3. Write queries to answer following questions:
Which type of crime is most occurring for each year?  List top 10 crimes for each year.
Which locations are most likely for a crime to happen?  List top 10 locations.
Are there certain high crime rate locations for certain crime types?
```

## Solution 2
- Create DataFrame from table
```
val crime_parquet_df=spark.read.table("chicago.crime_parquet")
crime_parquet_df.printSchema
crime_parquet_df.show(truncate=false)
```
```
val crime_parquet_16_20_df=crime_parquet_df.where("yr>=2016 and yr<=2020")
crime_parquet_16_20_df.printSchema
crime_parquet_16_20_df.show(truncate=false)
```


- Write queries to answer following questions:

- Which type of crime is most occurring for each year?  List top 10 crimes for each year. (use union)
```
val crime_parquet_16_df=crime_parquet_16_20_df.where("yr=2016").select("primary_type","yr").groupBy("primary_type", "yr").count().orderBy(desc("count")).limit(10)
crime_parquet_16_df.show
val crime_parquet_17_df=crime_parquet_16_20_df.where("yr=2017").select("primary_type","yr").groupBy("primary_type", "yr").count().orderBy(desc("count")).limit(10)
crime_parquet_17_df.show
val crime_parquet_18_df=crime_parquet_16_20_df.where("yr=2018").select("primary_type","yr").groupBy("primary_type", "yr").count().orderBy(desc("count")).limit(10)
crime_parquet_18_df.show
val crime_parquet_19_df=crime_parquet_16_20_df.where("yr=2019").select("primary_type","yr").groupBy("primary_type", "yr").count().orderBy(desc("count")).limit(10)
crime_parquet_19_df.show
val crime_parquet_20_df=crime_parquet_16_20_df.where("yr=2020").select("primary_type","yr").groupBy("primary_type", "yr").count().orderBy(desc("count")).limit(10)
crime_parquet_20_df.show
```
```
val crime_parquet_union_df=crime_parquet_16_df.union(crime_parquet_17_df).union(crime_parquet_18_df).union(crime_parquet_19_df).union(crime_parquet_20_df)
crime_parquet_union_df.show(false)
```
![3](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q2_1.png)


or
```
import org.apache.spark.sql.expressions.Window
val df = spark.read.table("chicago.crime_parquet")
val df1 = df.filter("yr>=2016").filter("yr<=2020")
val df2 = df1.groupBy("primary_type","yr").agg(count("id").as("number"))
val w1 = Window.partitionBy("yr").orderBy(desc("number"))
val df3 = df2.withColumn("rk",rank().over(w1))
val df4 = df3.filter("rk<=10").orderBy("yr","rk")
df4.show
```





- Which locations are most likely for a crime to happen?  List top 10 locations.
```
val crime_parquet_loc_df=crime_parquet_16_20_df.select("district").groupBy("district").count().orderBy(desc("count")).limit(10)
crime_parquet_loc_df.show
```
![4](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q2_2.png)

or
```
val df = spark.read.table("chicago.crime_parquet")
val df1 = df.filter(expr("yr<=2020") && expr("yr>=2016"))
val df2 = df1.groupBy("district").agg(count("id").as("number"))
val w1 = Window.orderBy(desc("number"))
val df3 = df2.withColumn("rk",rank().over(w1))
val df4 = df3.filter("rk <= 10")
df4.show
```




- Are there certain high crime rate locations for certain crime types? (use two columns to group by)
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
```
```
val w1=Window.partitionBy("district").orderBy(desc("count"))
val crime_parquet_loc_type_rank_df=crime_parquet_loc_type_df.withColumn("rk",rank() over(w1)).select("district","primary_type","count","rk").where("rk <=3").orderBy("district","rk")
crime_parquet_loc_type_rank_df.show
```
![5](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q2_3.png)





## Question 3
In retail_db, there are 6 tables.  Get yourself familiar with their schemas.  
categories  
customers  
departments  
orders  
order_items  
products

```
Write queries to answer following questions:
1. List all orders with total order_items = 5.
2. List customer_id, order_id, order item_count with total order_items = 5
3. List customer_fname，customer_id, order_id, order item_count with total order_items = 5 (join orders, order_items, customers table)
4. List top 10 most popular product categories. (join products, categories, order_items table)
5. List top 10 revenue generating products. (join products, orders, order_items table)
```


## Solution 3


- Use Hive server2 by beeline command tool
```
ssh pan@54.86.193.122
ssh pan@ip-172-31-92-98.ec2.internal
beeline
!connect 'jdbc:hive2://ip-172-31-92-98.ec2.internal:10000'
```


- Check Schemas of tables
```
show databases;
use retail_db;
show tables;
Describe formatted categories;
Describe formatted customers;
Describe formatted departments;
Describe formatted orders;
Describe formatted order_items;
DESCRIBE formatted products;
```


- Create DataFrame from table
```
val customers_df=spark.read.table("retail_db.customers")
customers_df.printSchema
customers_df.show(truncate=false)
```
```
val orders_df=spark.read.table("retail_db.orders")
orders_df.printSchema
orders_df.show(truncate=false)
```


- Spark read parquet file into DataFrame
```
val path1="/user/hive/warehouse/retail_db.db/categories"
val categories_df=spark.read.format("parquet").load(path1)
categories_df.printSchema
categories_df.show(truncate=false)
```
```
val path2="/user/hive/warehouse/retail_db.db/departments"
val departments_df=spark.read.format("parquet").load(path2)
departments_df.printSchema
departments_df.show(truncate=false)
```
```
val path3="/user/hive/warehouse/retail_db.db/order_items"
val order_items_df=spark.read.format("parquet").load(path3)
order_items_df.printSchema
order_items_df.show(truncate=false)
```
```
val path4="/user/hive/warehouse/retail_db.db/products"
val products_df=spark.read.format("parquet").load(path4)
products_df.printSchema
products_df.show(truncate=false)
```


- List all orders with total order_items = 5.
```
val order_items_df_sum=order_items_df.groupBy("order_item_order_id").sum("order_item_quantity").withColumnRenamed("sum(order_item_quantity)","sum_order_item_quantity").filter("sum_order_item_quantity=='5'").orderBy("order_item_order_id")
order_items_df_sum.show
```
or
```
val order_items_df_sum=order_items_df.groupBy("order_item_order_id").agg(sum("order_item_quantity").as("total")).filter("total=5").orderBy("order_item_order_id")
order_items_df_sum.show
```
or
```
order_items_df.groupBy("order_item_order_id").sum("order_item_quantity").filter("sum(order_item_quantity)=5").show
```
![6](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q3_1.png)


- List customer_id, order_id, order item_count with total order_items = 5
```
val order_items_df_sum=order_items_df.groupBy("order_item_order_id").sum("order_item_quantity").withColumnRenamed("sum(order_item_quantity)","sum_order_item_quantity").filter("sum_order_item_quantity=='5'").orderBy("order_item_order_id")
order_items_df_sum.show
```
```
orders_df.join(order_items_df_sum, orders_df("order_id")===order_items_df_sum("order_item_order_id"),"inner").join(customers_df, customers_df("customer_id")===orders_df("order_customer_id"),"inner").select("customer_id","order_id","sum_order_item_quantity").show
```
![7](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q3_2.png)


- List customer_fname，customer_id, order_id, order item_count with total order_items = 5 (join orders, order_items, customers table)
```
val order_items_df_sum=order_items_df.groupBy("order_item_order_id").sum("order_item_quantity").withColumnRenamed("sum(order_item_quantity)","sum_order_item_quantity").filter("sum_order_item_quantity=='5'").orderBy("order_item_order_id")
order_items_df_sum.show
```
```
orders_df.join(order_items_df_sum, orders_df("order_id")===order_items_df_sum("order_item_order_id"),"inner").join(customers_df, customers_df("customer_id")===orders_df("order_customer_id"),"inner").select("customer_fname","customer_id","order_id","sum_order_item_quantity").show
```
![8](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q3_3.png)


- List top 10 most popular product categories. (join products, categories, order_items table)
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
```
```
val join_df=products_df.join(categories_df, products_df("product_category_id")===categories_df("category_id"),"inner").join(order_items_df, order_items_df("order_item_product_id")===products_df("product_id"),"inner")
join_df.printSchema
```
```
val w1=Window.partitionBy("category_name")
val join_df_sum=join_df.withColumn("sum",sum(col("order_item_quantity")).over(w1)).orderBy(desc("sum")).select("category_name","sum").distinct
join_df_sum.take(10).foreach(println)
```
![9](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q3_4.png)


- List top 10 revenue generating products. (join products, orders, order_items table)
```
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
```
```
val join_df=orders_df.join(order_items_df, orders_df("order_id")===order_items_df("order_item_order_id"),"inner").join(products_df, products_df("product_id")===order_items_df("order_item_product_id"),"inner")
join_df.printSchema
```
```
val w1=Window.partitionBy("product_name")
val join_df_sum=join_df.withColumn("sum",sum(col("order_item_subtotal")).over(w1)).orderBy(desc("sum")).select("product_name","sum").distinct
join_df_sum.take(10).foreach(println)
```
![10](https://github.com/PAN-0921/ascending-hw/blob/master/pictures/W6_Q3_5.png)

