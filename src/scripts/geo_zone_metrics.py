import os
import sys
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
import findspark
findspark.init()
findspark.find()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
import pyspark.sql.window as Window
import geo_utils as gu
import geo_data_transformations


def main():

    date = sys.argv[1]
    depth =  sys.argv[2]
    basic_input_path = sys.argv[3] #basic_input_path = "/user/sushkos/data/geo/events"
    output_base_path = sys.argv[4] #output_base_path = /user/sushkos/data/analytics/geo/geo_zone_metrics/
    checkpoint_dir = sys.argv[5] #checkpoint_dir = "/user/sushkos/data/checkpoints/geo/geo_zone_metrics"
    basePath = sys.argv[6] #basePath =  "/user/sushkos/data/geo/events/"
    input_guides_path = sys.argv[7] #input_guides_path = "/user/sushkos/data/guides/"
    #studentusername = "sushkos"

    spark = SparkSession.builder \
                    .master("yarn") \
                    .appName("update_geo_zone_metrics_session") \
                    .getOrCreate()
    
    spark.sparkContext.setCheckpointDir(checkpoint_dir) 

    #!hdfs dfs -rm -r "/user/sushkos/data/geo"
    #!hdfs dfs -rm -r "/user/sushkos/data/analytics"
    #!hdfs dfs -rm -r "/user/sushkos/data/checkpoints/geo/geo_zone_metrics"

    gtu = geo_data_transformations.GeoTransformer()

    geo_cities = gu.get_cities(spark, input_guides_path,"geo.csv") #Читаем csv с городами

    paths = gu.input_paths(basic_input_path,date, depth ,True) #Создаем список путей на чтение из prod

    geo_events = gu.get_events(spark, basePath , paths) #Читаем events из prod

    geo_min_distance = gtu.calculate_events_nearest_city(geo_events, geo_cities).checkpoint() #Рассчитываем город последнего события пользователя

    #Считаем метрики за неделю
    geo_zone_week = geo_min_distance\
        .select('date','event_type','zone_id')\
        .withColumn('month',F.month('date'))\
        .withColumn('week',F.weekofyear('date'))\
        .groupBy(['month','week','zone_id'])\
        .pivot('event_type',['message','reaction','subscription','registration'])\
        .agg(F.expr("count(event_type)"))\
        .selectExpr('month','week','zone_id'\
                    ,'message as week_message'\
                    ,'reaction as week_reaction'\
                    ,'subscription as week_subscription'\
                    ,'registration as week_user')
    
    #Считаем метрики за месяц
    geo_zone_month = geo_min_distance\
        .select('date','event_type','zone_id')\
        .withColumn('month',F.month('date'))\
        .groupBy(['month','zone_id'])\
        .pivot('event_type',['message','reaction','subscription','registration'])\
        .agg(F.expr("count(event_type)"))\
        .selectExpr('month','zone_id'\
                    ,'message as month_message'\
                    ,'reaction as month_reaction'\
                    ,'subscription as month_subscription'\
                    ,'registration as month_user')

    #Объединяем в витрину
    geo_zone_metrics = geo_zone_week\
        .join(geo_zone_month,['month','zone_id'],how='inner')\
        .select('month','week','zone_id'\
                                ,'week_message','week_reaction','week_subscription','week_user'\
                                ,'month_message','month_reaction','month_subscription','month_user')\
        .orderBy('month','week','zone_id')


    geo_zone_metrics.write.mode("overwrite").parquet(f"{output_base_path}date={date}") # Записываем результат в analytics


if __name__ == "__main__":
        main()