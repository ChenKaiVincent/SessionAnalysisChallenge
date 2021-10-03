# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, IntegerType, StructType, StructField
from pyspark.sql.window import Window
from pyspark.sql.functions import *

def enrich_data(df):
    return df.withColumn("client_ip", split(col("client:port"), ":").getItem(0)) \
             .withColumn("url", split(col("request"), " ").getItem(1)) \
             .withColumn("timestamp", unix_timestamp("time", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"))

def filter_unsuccessful_requests(df):
    return df.filter(col("ssl_cipher") != "-1") \
             .filter(col("request_processing_time") != -1) \
             .filter(col("backend_processing_time") != -1) \
             .filter(col("response_processing_time") != -1)

def generate_session(df, time_window_ms):
    ip_window = Window.partitionBy("client_ip").orderBy("timestamp")
    prev_ts_df = df.withColumn("prev_timestamp", lag("timestamp", 1).over(ip_window))
    ip_window_df = prev_ts_df.withColumn("new_session",
                                         when((col("prev_timestamp").isNull())
                                            | (col("timestamp") - col("prev_timestamp") > time_window_ms), True) \
                                             .otherwise(False))\
                                             .withColumn("ts_diff", col("timestamp") - col("prev_timestamp"))
    session_id_df = ip_window_df.withColumn("session_id",
                                            sum(col("new_session").cast("long")).over(ip_window))
    session_df = session_id_df.withColumn("session",
                                          sha2(concat(col("client_ip"), col("session_id").cast("String")), 256))
    return session_df

def get_session_time(session_df):
    return session_df.groupBy(["client_ip", "session"]) \
                             .agg(min(session_df.timestamp).alias("min_timestamp"),
                                  max(session_df.timestamp).alias("max_timestamp")) \
                             .withColumn("session_time", col("max_timestamp") - col("min_timestamp")) #\
                             #.drop("min_timestamp") \
                             #.drop("max_timestamp")

if __name__ == "__main__":
    file_name = sys.argv[1]
    time_window_ms = 15 * 60

    print("file name: %s" % (file_name))

    # create spark session
    spark = SparkSession \
            .builder \
            .appName("session handle") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()

    # read data
    schema = StructType([
                  StructField("time", StringType(), True),
                  StructField("elb", StringType(), True),
                  StructField("client:port", StringType(), True),
                  StructField("backend:port", StringType(), True),
                  StructField("request_processing_time", FloatType(), True),
                  StructField("backend_processing_time", FloatType(), True),
                  StructField("response_processing_time", FloatType(), True),
                  StructField("elb_status_code", IntegerType(), True),
                  StructField("backend_status_code", IntegerType(), True),
                  StructField("received_bytes", IntegerType(), True),
                  StructField("sent_bytes", IntegerType(), True),
                  StructField("request", StringType(), True),
                  StructField("user_agent", StringType(), True),
                  StructField("ssl_cipher", StringType(), True),
                  StructField("ssl_protocol", StringType(), True)
                ])

    df = spark.read \
              .option("delimiter", " ") \
              .option("header", False) \
              .csv(file_name, schema=schema)
    df.printSchema()

    # preprocessing: enrich data
    df = enrich_data(df)

    # preprocessing: filter out unsuccessfull requests
    df = filter_unsuccessful_requests(df)

    # preprocessing: drop unneccessary columns
    df = df.drop("time",
                 "elb", 
                 "client:port", 
                 "backend:port", 
                 "request_processing_time", 
                 "backend_processing_time", 
                 "response_processing_time", 
                 "elb_status_code", 
                 "backend_status_code", 
                 "received_bytes", 
                 "sent_bytes", 
                 "user_agent",
                 "ssl_cipher", 
                 "ssl_protocol")

    # preprocessing: get sessoin
    session_df = generate_session(df, time_window_ms)

    # preprocessing: drop unnecessary columns
    session_df = session_df.drop("request",
                                 "prev_timestamp", 
                                 "new_session", 
                                 "session_id", 
                                 "ts_diff")

    # analysis 1: Sessionize the web log by IP. (aggregrate all page hits by visitor/IP during a session.)
    print("analysis 1: Sessionize the web log by IP. (aggregrate all page hits by visitor/IP during a session.)")
    sess_all_page_hits_df = session_df.groupBy(["client_ip", "session"]).agg(count("url"))
    sess_all_page_hits_df.show(10, False)
   
    # analysis 2: Determine the average session time
    print("analysis 2: Determine the average session time")
    sess_time_df = get_session_time(session_df)
    avg_sess_time_df = sess_time_df.agg(avg("session_time"))
    avg_sess_time_df.show(10, False)

    # analysis 3: Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    print("analysis 3: Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.")
    sess_uniq_url_hits_df = session_df.groupBy(["client_ip", "session"]).agg(countDistinct("url"))
    sess_uniq_url_hits_df.show(10, False)

    # analysis 4: Find the most engaged users, ie the IPs with the longest session times
    print("analysis 4: Find the most engaged users, ie the IPs with the longest session times")
    max_sess_time_df = sess_time_df.agg(max("session_time"))
    max_sess_time_df.show(10, False)

    spark.stop()
