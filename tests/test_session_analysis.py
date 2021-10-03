# -*- coding: utf-8 -*-

import pytest
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType, FloatType, IntegerType, StructType, StructField
from pyspark.sql.window import Window
from session_analysis.session_analysis import enrich_data, filter_unsuccessful_requests, generate_session, get_session_time

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

@pytest.fixture(scope="session")
def spark_test_session():
    return (
        SparkSession
        .builder
        .master('local[*]')
        .appName('unit-testing')
        .getOrCreate()
    )

def are_dfs_equal(df1, df2):
    if df1.schema != df2.schema:
        return False
    if df1.collect() != df2.collect():
        return False
    return True

def test_enrich_data(spark_test_session):
    """
    Test if the new generated columns ["client_ip", "url", "timestamp"] are correct.
    :param spark_test_session
    :return: boolean
    """
    df = spark_test_session.read \
                           .option("delimiter", " ") \
                           .option("header", False) \
                           .csv('tests/resources/test_data_1.log', schema=schema)
    res_df = enrich_data(df).select(["client_ip", "url", "timestamp"])
    exp_values = [("100.100.100.100", "https://abc.com:443/shop/login?a=foo&b=bar", 1437526828)]
    exp_df = spark_test_session.createDataFrame(exp_values, ["client_ip", "url", "timestamp"])
    assert are_dfs_equal(exp_df, res_df)

def test_filter_unsuccessful_requests(spark_test_session):
    """
    Test if all unsuccessful requests can be filtered out.
    (i.e. success request: ssl_cipher != "-1" and all "*_processing_time != -1)
    :param spark_test_session
    :return: boolean
    """
    df = spark_test_session.read \
                           .option("delimiter", " ") \
                           .option("header", False) \
                           .csv('tests/resources/test_data_2.log', schema=schema)
    res_df = filter_unsuccessful_requests(df).select("client:port")
    exp_values = [{"client:port": "100.100.100.100:51582"}]
    exp_df = spark_test_session.createDataFrame(exp_values)
    assert are_dfs_equal(exp_df, res_df)

def test_generate_session(spark_test_session):
    """
    Test if the generated session (sha256) strings are correct based on the specific time window.
    :param spark_test_session
    :return: boolean
    """
    time_window_ms = 15 * 60
    df = spark_test_session.read \
                           .option("delimiter", " ") \
                           .option("header", False) \
                           .csv('tests/resources/test_data_3.log', schema=schema)
    enr_df = enrich_data(df)
    res_df = generate_session(enr_df, time_window_ms).select("session")
    exp_values = [{"session": "7c7fbd3e009638314365e39ad5b18502c97347f3ea4b4e6d8a9bf95acb7065b2"},
                  {"session": "7c7fbd3e009638314365e39ad5b18502c97347f3ea4b4e6d8a9bf95acb7065b2"},
                  {"session": "d2efcc94f416878fafaaa2d19a7bd290ad56ba0ed4914d9b541708939aa061c9"},
                  {"session": "d2efcc94f416878fafaaa2d19a7bd290ad56ba0ed4914d9b541708939aa061c9"},
                  {"session": "c9c4852547b7837200a9f67ce881d1bc37efdd7cc7122220fb9829568de454cd"}]
    exp_df = spark_test_session.createDataFrame(exp_values)
    assert are_dfs_equal(exp_df, res_df)

def test_get_session_time(spark_test_session):
    """
    Test if the session time for each client ip is correct based on the generated session strings.
    (p.s. session time is 0 if only 1 request in the log)
    :param spark_test_session
    :return: boolean
    """
    time_window_ms = 15 * 60
    df = spark_test_session.read \
                           .option("delimiter", " ") \
                           .option("header", False) \
                           .csv('tests/resources/test_data_3.log', schema=schema)
    enr_df = enrich_data(df)
    sess_df = generate_session(enr_df, time_window_ms)
    sess_time_df = get_session_time(sess_df).select(["client_ip", "session_time"])
    exp_values = [{"client_ip": "100.100.100.100", "session_time": 840},
                  {"client_ip": "100.100.100.100", "session_time": 120},
                  {"client_ip": "100.100.100.100", "session_time": 0}]
    exp_df = spark_test_session.createDataFrame(exp_values)
    assert are_dfs_equal(exp_df, sess_time_df)

