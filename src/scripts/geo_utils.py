import datetime
import math
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
import pyspark.sql.window as Window

def input_paths(basic_input_path: str, date: str, depth: str, IsAllEvents: bool, event_type = ""):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')

    if IsAllEvents:
        event_str= ''
    else:
        event_str = "/event_type="+event_type
        
    return [f"{basic_input_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}{event_str}" for x in range(int(depth))]

def get_cities(spark : SparkSession, path: str, filename: str):

    pi_rad = math.pi/180

    return spark.read.option("delimiter", ";")\
            .option("header", True)\
            .csv(path+filename)\
            .withColumn('lat', (F.regexp_replace('lat',',','.').cast("double"))*pi_rad)\
            .withColumn('lng', (F.regexp_replace('lng',',','.').cast("double"))*pi_rad)\
            .selectExpr(['id as zone_id','city', 'lat as city_lat_rad','lng as city_lon_rad'])

def get_timezones(spark : SparkSession, path: str, filename: str):
    return  spark.read.option("delimiter", ";")\
                .option("header", True)\
                .csv(path+filename)

def get_events(spark: SparkSession,basePath: str, read_path, isFullEvents = True, event_type=''):
    if isinstance(read_path, str):
        read_path = [read_path]

    if isFullEvents:
        return spark.read\
            .option("basePath", basePath)\
            .parquet(*read_path)
    else: 
        return spark.read\
            .option("basePath", basePath)\
            .parquet(*read_path)\
            .where(f"event_type='{event_type}'")

    

def load_events_to_prod(df,output_path):
       df.write.option("header",True) \
            .mode("overwrite") \
            .partitionBy("date","event_type") \
            .parquet(output_path)



