from pyspark.sql import SparkSession 
from pyspark.sql.functions import col
from connection import * 
from scripts import * 
import os 
from dotenv import load_dotenv


spark  =SparkSession.builder\
    .appName("sales-analytics")\
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.2") \
    .config("spark.driver.extraClassPath", "/home/mcmac/prj/sales/sales-analytics/jars/spark-snowflake_2.12-2.12.0-spark_3.2.jar,") \
    .getOrCreate()


sfOptions = {
    "sfURL": "https://af71825.ap-southeast-1.snowflakecomputing.com",
    "sfUser": "devTest1",
    "sfPassword": "Playstation@6",
    "sfDatabase": "SRC_DB",
    "sfSchema": "SRC_SCHEMA",
    "sfWarehouse": "COMPUTE_WH",
}


def data_loading(spark , sfOptions):
    df = spark.read.json("/home/mcmac/prj/sales/sales-analytics/data/sales.json")
    cleanDF = df.select(col("_id.$oid").alias('_id') ,\
                        col('couponUsed') ,\
                        col('customer.age.$numberInt').alias('user_age'),\
                        col('customer.email').alias('user_email') ,\
                        col('customer.gender').alias('user_gender'),\
                        col('customer.satisfaction.$numberInt').alias('user-satisfaction'),\
                        col('purchaseMethod').alias('purchase_method'),\
                        col('saleDate.$date.$numberLong').cast('date').alias('saleDate'),\
                        col('storeLocation').alias('store_location'))

    saleData = df.select(col("_id.$oid").alias('_id') ,\
                        explode(df.items).alias('items')
                        )

    saleData = saleData.select(col('_id') ,\
                        col('items.name').alias('item_name'),\
                        col('items.tags').alias('items_tags'),\
                        col('items.price.$numberDecimal').alias('items_price'),\
                        col('items.quantity.$numberInt').alias('items_qty')
                        )
    
    tagsDF = saleData.select(col('_id'),\
                        explode(saleData.items_tags).alias('items_tags'))
    tagsDF.show()
    saleData.show()
    print('Loading data into tables : ')
    loadDataIntoTbl(spark, sfOptions, saleData, 'salesdata')
    loadDataIntoTbl(spark, sfOptions, cleanDF, 'userdata')
    loadDataIntoTbl(spark, sfOptions, tagsDF , 'tagsdata')
    print('Data Load complete : ')

def starter_method():
    print('Starting Process : ')
    spark = sparkInit()
    sfOptions = getSparkSFOptions()
    data_loading(spark , sfOptions)

if __name__ == "__main__":
    load_dotenv()
    starter_method()