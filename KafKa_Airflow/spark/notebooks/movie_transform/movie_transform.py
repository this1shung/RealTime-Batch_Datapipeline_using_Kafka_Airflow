from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.window import Window 
from pyspark.sql.functions import *
import os
import calendar

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("ShowData") \
        .config("fs.s3a.access.key", "minio") \
        .config("fs.s3a.secret.key", "minio123") \
        .config("fs.s3a.endpoint", "http://host.docker.internal:9000") \
        .config("fs.s3a.connection.ssl.enabled", "false") \
        .config("fs.s3a.path.style.access", "true") \
        .getOrCreate()

    current_month = datetime.now().month
    
    df_schema = spark.read.option("header", "true").csv("s3a://doanbucket-lqh/data/Sample.csv")
    df_map = spark.read.option("header", 'true').csv(f"s3a://doanbucket-lqh/map/t{current_month}/MapT{current_month}.csv")
    df_map = df_map.dropDuplicates()
    df_map1 = spark.read.option("header", 'true').csv(f"s3a://doanbucket-lqh/map/t{current_month}/usermap.csv")
    df_map2 = spark.read.option("header", 'true').csv(f"s3a://doanbucket-lqh/map/t{current_month}/regionmap.csv")
    
    df = spark.read.option("header", "true").csv(f"s3a://doanbucket-lqh/data/t{current_month}/*")

    df = df.withColumn("Day_of_week",dayofweek(col("createdat")))\
    .withColumn("hour_of_day",hour(col("createdat")))

    df = df.withColumn("is_weekend", when((col("Day_of_week") == 1) | (col("Day_of_week") == 7), True).otherwise(False))

    df = df.withColumn("time_period",
    when((col("hour_of_day") >= 0) & (col("hour_of_day") < 6), "Night") \
    .when((col("hour_of_day") >= 6) & (col("hour_of_day") < 12), "Morning") \
    .when((col("hour_of_day") >= 12) & (col("hour_of_day") < 18), "Afternoon") \
    .otherwise("Evening")
    )

    df = df.withColumn("activity_date", to_date("createdat"))

    count_active_date = df.groupby("userid").agg(countDistinct("activity_date").alias("Number_Active_Date"))

    df = df.join(count_active_date, on="userid", how='left')

    df = df.withColumn('Number_Active_Date', coalesce(col('Number_Active_Date'), lit(0)))

    current_month = datetime.now().month
    current_year = datetime.now().year
    days_in_current_month_value = calendar.monthrange(current_year, current_month)[1]

    df = df.withColumn('Activeness', format_number((col("Number_Active_Date") / 30) * 100, 2))

    df = df.withColumn("month", month(col('activity_date')))

    df = df.drop("cassandra_timestamp", "createdat", "searchtype")

    df = df.groupBy('userid', 'keyword', 'Number_Active_Date', 'Activeness', 'month','is_weekend','time_period').count()

    df = df.withColumnRenamed('count', 'TotalSearch')

    window = Window.partitionBy('userid').orderBy(col('TotalSearch').desc())

    df = df.withColumn('Rank', row_number().over(window))

    df = df.filter(col('Rank') == 1)

    df = df.withColumnRenamed('keyword', 'Most_Search')

    df = df.join(df_map, on="Most_Search", how='left')

    df = df.join(df_map1,on="userid",how='inner')

    df = df.join(df_map2,on="location",how='inner')

    df.show()

    output_path = f"s3a://doanbucket-lqh/processed/t{current_month}/"

    df.write \
        .option("header", "true") \
        .csv(f"{output_path}data.csv", mode="overwrite")

    print(f"Data saved to {output_path}data.csv successfully")

    spark.stop()
