from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import col, year, month, dayofmonth
from pyspark.sql import Row

spark = SparkSession.builder.config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.1.0').getOrCreate()

def get_latest_etl_time():
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table= "cdc_tracking", keyspace='netflix_keyspace').load()
    latest_etl_time = df.agg({'last_etl_time':'max'}).take(1)[0][0]
    return latest_etl_time if latest_etl_time else None

def get_latest_data_time():
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table='user_search_history', keyspace='netflix_keyspace').load()
    cassandra_latest_time = data.agg({'createdat':'max'}).take(1)[0][0]
    return cassandra_latest_time if cassandra_latest_time else None

def main_task(latest_etl_time):

    if latest_etl_time is None:
        df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table='user_search_history', keyspace='netflix_keyspace') \
        .load()
    else:
        df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table='user_search_history', keyspace='netflix_keyspace') \
        .load().where(col('createdat') >= latest_etl_time)

    df = df.withColumn("year",year(col("createdat")))\
        .withColumn("month",month(col("createdat")))\
        .withColumn("day",dayofmonth(col("createdat")))
    
    final_df = df.drop("cassandra_timestamp")


    etl_time = final_df.agg({'createdat':'max'}).take(1)[0][0]
    count = final_df.count()
    data = [Row(id = 1001, last_etl_time=etl_time, num_records_processed=count)]
    df_cas = spark.createDataFrame(data)
    df_cas.write.format("org.apache.spark.sql.cassandra").options(table="cdc_tracking", keyspace="netflix_keyspace").mode("append").save()

    final_df.write.partitionBy('year','month','day').mode('append').format('parquet').save("D:\PROJECT\RealTime-Batch_Datapipeline_using_Kafka_Airflow\Data")


if __name__ == "__main__":
    latest_cas_time = get_latest_data_time()
    latest_etl_time = get_latest_etl_time()

    if latest_etl_time is None:
        print("CDC tracking is empty. Running main task for the first load.")
        main_task(latest_etl_time)
    else:
        if latest_cas_time and latest_etl_time:
            if latest_cas_time  > latest_etl_time:
                print("Running main task: new data found.")
                main_task(latest_etl_time)
            else:
                print("No new data found.")
        else:
            print("One of the timestamps is None.")