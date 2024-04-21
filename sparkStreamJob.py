""" 
A streaming job that converts a csv/text file into mysql db using Spark and Kafka
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, current_timestamp, column
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType, BooleanType
def streamJob(topic):
    schemas = { #choosing the schema according to the topic
        "users":[
            StructField('id', LongType(), True), 
            StructField('name', StringType(), True),
            StructField('screen_name', StringType(), True),
            StructField('location', StringType(), True),
            StructField('url', StringType(), True),
            StructField('description', StringType(), True),
            StructField('verified', BooleanType(), True),
            StructField("followers_count", LongType(), True),
            StructField("friends_count",LongType(), True),
            StructField("listed_count", LongType(), True),
            StructField("favourites_count", LongType(), True),
            StructField("statuses_count", LongType(), True),
            StructField("created_at", StringType(), True)
        ],
        "tweet":[
            StructField('id', LongType(), True), 
            StructField('user', StructType([
                StructField('id', LongType(), True)
        ]), True),
            StructField('text', StringType(), True),
            StructField('retweet_count', LongType(), True),
            StructField('favorite_count', LongType(), True),
            StructField('lang', StringType(), True),
            StructField('retweeted', BooleanType(), True),
            StructField("created_at", StringType(), True)
        ],
        "retweet":[
            StructField('id', LongType(), True),
            StructField('retweeted_status', StructType([
                StructField('id', LongType(), True)
            ]), True),
            StructField('user', StructType([
                StructField('id', LongType(), True)
            ]), True),
            StructField('text', StringType(), True),
            StructField('retweet_count', LongType(), True),
            StructField('favorite_count', LongType(), True),
            StructField('lang', StringType(), True),
            StructField('retweeted', BooleanType(), True),
            StructField("created_at", StringType(), True)
        ],
    }
    
    def doNothing(df): #no modifications have to be made to the schema
        return df
    
    def modifyTweetDataframe(df): #tweet and retweet dataframes have to be modified to remove nesting
        return df.withColumn("user_id",column("user.id")).drop("user")
    def modifyRetweetDataframe(df):
        newdf = df.withColumn("user_id",column("user.id")).drop("user")
        return newdf.withColumn("original_id",column("retweeted_status.id")).drop("retweeted_status")
    
    dataFrameModifierDict = { #topic-callback mapping to modify schema according to topic
        'users': doNothing,
        'tweet' : modifyTweetDataframe,
        'retweet' : modifyRetweetDataframe
    }
    def writeToTable(df, tableName): #Writing the dataframe to mysql table
            return df.write \
            .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/dbt_twitter") \
    .option("dbtable", tableName) \
    .option("user", "dbt") \
    .option("password", "dbt123") \
        .mode("append") \
    .save()

    def save_to_mysql(current_df, epoch_id):
        print("Printing epoch_id: ", epoch_id)
        writeToTable(current_df, topic)
        current_df_final = current_df
    
    #starting a spark session
    spark = SparkSession \
        .builder \
        .appName("Streaming from Kafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.3.0') \
        .config("spark.sql.shuffle.partitions", 4) \
        .master("local[*]") \
        .getOrCreate()

    #choosing correct schema
    json_schema = StructType(schemas[topic])

    #subscribing to kafka topic to retrieve raw data
    streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
            .option("includeHeaders", "true")\
                .load()
    
    #taking each streamed line as a string    
    json_df = streaming_df.selectExpr("cast(value as string) as value")

    #value contains the actual data of the message. using .select(value.*) we obtain the json heirarchy
    json_expanded_df = json_df \
    .withColumn("value", from_json(json_df["value"], json_schema)) \
    .select("value.*") \
    .dropDuplicates(["id"]) \
    .withColumn("created_at",to_timestamp("created_at")) \
    .withColumn("inserted_at", current_timestamp())
    
    #modifying the schema according to the correct callback
    json_expanded_df = dataFrameModifierDict[topic](json_expanded_df)

    #writing the dataframe to mysql
    print("Created expanded dataframe...")
    json_expanded_df.writeStream \
    .outputMode("update") \
    .foreachBatch(save_to_mysql) \
    .start() \
    .awaitTermination()

if __name__ == "__main__": #Selecting the topic for streaming
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic')
    args = parser.parse_args()
    topic = args.topic
    streamJob(topic)
    