# import os

# import boto3
from lib2to3.pgen2.pgen import DFAState
from zlib import DEF_BUF_SIZE
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, FloatType, TimestampType
from pyspark.sql.functions import from_json,col


audio_data = "/mnt/10ac-batch-5/week9/chang/kafka"
topic_input = "groupHu_audio"

import findspark
findspark.init('/opt/spark')

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell'



def main():

    spark = SparkSession \
        .builder \
        .master('local[1]') \
        .appName("kafka-streaming-audio") \
        .config("spark.cassandra.connection.host","172.0.0.4")\
        .config("spark.cassandra.connection.port","9042")\
        .config("spark.cassandra.auth.username","cassandra")\
        .config("spark.cassandra.auth.password","cassandra")\
        .config("spark.driver.host", "localhost")\
        .getOrCreate()

    df_audio = read_from_kafka(spark)
    print(df_audio)
    df_audio.printSchema()
    audio_schema = StructType([
        StructField("data", StringType(), False),
        StructField("rate", IntegerType(), False),
        StructField("width", IntegerType(), False),
    ])

    df1 = df_audio.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),audio_schema).alias("data")).select("data.*")
    df1.printSchema()
    def writeToCassandra(writeDF, _):
            writeDF.write \
              .format("org.apache.spark.sql.cassandra")\
              .mode('append')\
              .options(table="audio_schema", keyspace="audio")\
              .save()

    df1.writeStream \
              .foreachBatch(writeToCassandra) \
              .outputMode("update") \
              .start()\
              .awaitTermination()
    df1.show()
    
    



    #summarize_sales(df_audio)







def read_from_kafka(spark):#, params):
    options_read = {
        "kafka.bootstrap.servers":
            "localhost:9092",
        "subscribe":
            topic_input,
    }

    df_audio = spark.readStream \
        .format("kafka") \
        .options(**options_read) \
        .option("startingOffsets", "earliest") \
        .load()

    return df_audio


# def summarize_sales(df_audio):
#     schema = StructType([
#         StructField("data", StringType(), False),
#         StructField("rate", IntegerType(), False),
#         StructField("width", IntegerType(), False),
#     ])

#     ds_audio = df_audio \
#         .selectExpr("CAST(value AS STRING)") \
#         .select(F.from_json("value", schema=schema).alias("data")) \
#         .writeStream \
#         .queryName("streaming_to_console") \
#         .trigger(processingTime="1 minute") \
#         .format("console") \
#         .option("numRows", 25) \
#         .option("truncate", False) \
#         .start()

#     ds_audio.awaitTermination()
if __name__ == "__main__":
    #if ran as a script we want to access the following functions
    main()