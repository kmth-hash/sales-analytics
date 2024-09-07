from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, explode
import os 
import snowflake.connector as sc
from dotenv import load_dotenv
from scripts import * 

load_dotenv()


def sparkInit():
    spark  =SparkSession.builder\
    .appName("sales-analytics")\
    .config("spark.jars.packages", os.getenv('SNOWFLAKE_JAR')) \
    .config("spark.driver.extraClassPath", os.getenv('SNOWFLAKE_JAR_PATH')) \
    .getOrCreate()

    return spark 

def getSparkSFOptions():
    return  {
        "sfURL": os.getenv("SNOW_URL"),
        "sfUser": os.getenv("SNOW_USER"),
        "sfPassword": os.getenv("SNOW_PASS"),
        "sfDatabase": os.getenv("SNOW_SOURCE_DB"),
        "sfSchema": os.getenv("SNOW_SRC_SCHEMA"),
        "sfWarehouse": os.getenv("SNOW_WAREHOUSE"),
    }

def snowOptions():
    op = {
        "account": os.getenv("SNOW_APP_ID"),
        "user": os.getenv("SNOW_USER"),
        "password": os.getenv("SNOW_PASS"),
        "database": os.getenv("SNOW_SOURCE_DB"),
        "schema": os.getenv("SNOW_SRC_SCHEMA"),
        "warehouse": os.getenv("SNOW_WAREHOUSE"),
    }    
    return  op

def snowConnect():
    op = {
        "account": os.getenv("SNOW_APP_ID"),
        "user": os.getenv("SNOW_USER"),
        "password": os.getenv("SNOW_PASS"),
        "database": os.getenv("SNOW_SOURCE_DB"),
        "schema": os.getenv("SNOW_SRC_SCHEMA"),
        "warehouse": os.getenv("SNOW_WAREHOUSE"),
    }
    conn = sc.connect(
        **op
    )    
    return conn.cursor()


