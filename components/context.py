import os
import sys
import json

from pyspark import SparkConf, SparkContext, SQLContext

def create_config(master_url='local', app_name='app', memory='1g', **kwargs):
    config = SparkConf().\
                setMaster(master_url).\
                setAppName(app_name).\
                set('spark.executor.memory', memory)
    return config

def create_context(spark_config):
    sc = SparkContext(conf=spark_config)
    sql_context = SQLContext(sparkContext=sc)
    return sc, sql_context