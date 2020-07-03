"""
Utility functions for the project
"""
import configparser
import sys

from pyspark.sql import SparkSession


def spark_init():
    """
    initialise spark
    :return: return spark context
    """
    spark = SparkSession \
        .builder \
        .appName('onfido') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark


def parse_conf(conffile):
    """
    function to read the config file
    :return: conf
    """
    if conffile is None:
        print('File Error: No File provided')
        sys.exit(-1)

    conf = configparser.ConfigParser()
    conf.optionxform = str
    conf.read(conffile)

    return conf
