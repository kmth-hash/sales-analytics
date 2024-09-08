from scripts import * 

def processed_data(spark ,snowSfOptions, snowConn) : 
    # top 10 customers query 
    query = fetchSQL('TOP_10_USERS')
    runSnowQuery(snowConn , query)

    # sales By store 
    query = fetchSQL('STORE_SALES')
    runSnowQuery(snowConn , query)

    # sbs = spark.read.format("snowflake") \
    # .options(**snowSfOptions) \
    # .option("query", "SELECT * FROM tbl_or_view") \
    # .load()
    
    # sbs.show()
