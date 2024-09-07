import os 
from dotenv import load_dotenv

load_dotenv()

def view_sf_data(description,resultSet) : 
    res = []
    headers = []
    for i in description : 
        # print('x-->',i)
        headers.append(i.name)
    res.append(headers)
    for i in resultSet : 
        # print('itr --> ',i,type(i))
        res.append(list(i))
    print(res)

def fetchSQL(sql_file_name) : 
    snowFilePath = os.getenv('SNOW_SQL_PATH')
    with open(f'{snowFilePath}/{sql_file_name}.sql','r') as fPtr : 
        query = ''
        for line in fPtr.read() : 
            query += line 
        print(query)
    return query 


def loadDataIntoTbl(spark , sfOptions, data , tbl) : 
    data.write.format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", tbl) \
        .mode("overwrite") \
        .save()
    print(f'{tbl} data populated : ')

def runSnowQuery(snowConn , query):
    print('Running query : ')
    res = snowConn.execute(query)   
    res = res.fetchall()
    return res 
