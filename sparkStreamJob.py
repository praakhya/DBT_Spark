import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, current_timestamp, column
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType, BooleanType
def streamJob(topic):
    schemas = {
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
    
    def doNothing(df):
        return df
    
    def modifyTweetDataframe(df):
        return df.withColumn("user_id",column("user.id")).drop("user")
    dataFrameModifierDict = {
        'users': doNothing,
        'tweet' : modifyTweetDataframe,
        'retweet' : modifyTweetDataframe
    }
    def writeToTable(df, tableName):
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
        #\
        #        .withColumn("processed_at", lit(processed_at))\
        #        .withColumn("batch_it", lit(epoc_id))
    """     current_df_final.write \
            .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/dbt_twitter") \
    .option("dbtable", "users") \
    .option("user", "dbt") \
    .option("password", "dbt123") \
        .mode("append") \
    .save()
    """
    
    spark = SparkSession \
        .builder \
        .appName("Streaming from Kafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.3.0') \
        .config("spark.sql.shuffle.partitions", 4) \
        .master("local[*]") \
        .getOrCreate()


    json_schema = StructType(schemas[topic])

    streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
            .option("includeHeaders", "true")\
                .load()
        
    json_df = streaming_df.selectExpr("cast(value as string) as value")

    json_expanded_df = json_df \
    .withColumn("value", from_json(json_df["value"], json_schema)) \
    .select("value.*") \
    .dropDuplicates(["id"]) \
    .withColumn("created_at",to_timestamp("created_at")) \
    .withColumn("inserted_at", current_timestamp())
    
    json_expanded_df = dataFrameModifierDict[topic](json_expanded_df)

    #json_expanded_df.writeStream.format("console").outputMode("append").start().awaitTermination()

    print("Created expanded dataframe...")
    json_expanded_df.writeStream \
    .outputMode("update") \
    .foreachBatch(save_to_mysql) \
    .start() \
    .awaitTermination()
    #        .foreachBatch(save_to_mysql) \
    #        .start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic')
    args = parser.parse_args()
    topic = args.topic
    streamJob(topic)
    