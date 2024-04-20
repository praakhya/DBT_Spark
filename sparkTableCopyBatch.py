import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, current_timestamp
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType, BooleanType

def batchCopyTableJob(tableName):
    startTime = datetime.now()
    spark = SparkSession \
        .builder \
        .appName("Streaming from Kafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.3.0') \
        .config("spark.sql.shuffle.partitions", 4) \
        .master("local[*]") \
        .getOrCreate()
    spark.read.format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/dbt_twitter") \
    .option("dbtable", tableName) \
    .option("user", "dbt") \
    .option("password", "dbt123") \
        .load() \
            .write.format("jdbc") \
                .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/dbt_twitter") \
    .option("dbtable", tableName + "_copy") \
    .option("user", "dbt") \
    .option("password", "dbt123") \
        .mode("append") \
    .save()
    endTime = datetime.now()
    time_format = "%d/%m/%Y %H:%M:%S"
    print(f"{tableName} copied to {tableName}_copy, Started at : {startTime.strftime(time_format)}, Ended at {endTime.strftime(time_format)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic')
    args = parser.parse_args()
    topic = args.topic
    batchCopyTableJob(topic)
    