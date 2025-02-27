import configparser
from pyspark import SparkConf


def get_conf(env):
    config = configparser.ConfigParser()
    config.read("/Users/Andriy_Bezpaliuk/Spark-Udemy/spark-project/conf/sbdl.conf")
    file_path = "/Users/Andriy_Bezpaliuk/Spark-Udemy/spark-project/conf/sbdl.conf"
    conf = {}
    for (key, val) in config.items(env):
        conf[key] = val

    return conf

def get_spark_conf(env):
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("/Users/Andriy_Bezpaliuk/Spark-Udemy/spark-project/conf/spark.conf")
    for (key, val) in config.items(env):
        spark_conf.set(key, val)

    return spark_conf

def get_data_filter(env, data_filter):
    conf = get_conf(env)
    return "true" if conf[data_filter] == "" else conf[data_filter]