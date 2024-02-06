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
    output_base_path = sys.argv[4] #output_base_path = /user/sushkos/data/analytics/geo/geo_recommendations/
    checkpoint_dir = sys.argv[5] #checkpoint_dir = "/user/sushkos/data/checkpoints/geo/geo_recommendations"
    basePath = sys.argv[6] #basePath =  "/user/sushkos/data/geo/events/"
    input_guides_path = sys.argv[7] #input_guides_path = "/user/sushkos/data/guides/"

    #studentusername = "sushkos"

    spark = SparkSession.builder \
                    .master("yarn") \
                    .appName("update_geo_recommendations_session") \
                    .getOrCreate()
    
    spark.sparkContext.setCheckpointDir(checkpoint_dir) 

    #!hdfs dfs -rm -r "/user/sushkos/data/geo"
    #!hdfs dfs -rm -r "/user/sushkos/data/analytics"
    #!hdfs dfs -rm -r "/user/sushkos/data/checkpoints/geo/geo_recommendations"

    gtu = geo_data_transformations.GeoTransformer()

    geo_cities = gu.get_cities(spark, input_guides_path,"geo.csv") #Читаем csv с городами

    geo_cities_tz = gu.get_timezones(spark, input_guides_path,"geo_tz.csv") #Читаем csv с таймзонами городов

    paths = gu.input_paths(basic_input_path,date, depth ,True) #Создаем список путей на чтение из prod

    geo_events = gu.get_events(spark, basePath , paths) #Читаем events из prod

    geo_users_full_deduplicated = gtu.calculate_user_pairs_by_conditions(geo_events).checkpoint()   #Рассчитываем DataFrame уникальных пар юзеров без сообщений и с общими каналами

    geo_min_distance = gtu.calculate_events_nearest_city(geo_events, geo_cities) #Рассчитываем город последнего события пользователя
    

    #Соединяем уникальные пару юзеров c последним событием для каждого пользователя из пары, находим пары из одного города, выбираем самый свежий datetime из пары, добавляем localtime
    geo_recommendations_filtered = geo_users_full_deduplicated\
        .join(geo_min_distance,geo_users_full_deduplicated.user_id_1==geo_min_distance.user_id,how='inner')\
        .selectExpr('user_id_1','user_id_2','datetime as datetime_1','zone_id as zone_id_1',
                    'event_lat_rad as event_lat_rad_1','event_lon_rad as event_lon_rad_1')\
        .join(geo_min_distance,geo_users_full_deduplicated.user_id_2==geo_min_distance.user_id,how='inner')\
        .selectExpr('user_id_1','user_id_2','datetime_1','zone_id_1','datetime as datetime_2', 'zone_id as zone_id_2',
                    'event_lat_rad_1','event_lon_rad_1','event_lat_rad as event_lat_rad_2','event_lon_rad as event_lon_rad_2')\
        .withColumn("event_sphere_distance",\
                        2*6371*F.asin(
                        (F.sqrt((F.pow(F.sin((F.col('event_lat_rad_1') - F.col('event_lat_rad_2'))/F.lit(2)),2))\
                        +F.cos('event_lat_rad_1')*F.cos('event_lat_rad_2')\
                        *(F.pow(F.sin((F.col('event_lon_rad_1') - F.col('event_lon_rad_2'))/F.lit(2)),2))                            
                        ))))\
        .filter(F.col('event_sphere_distance'<=1))
    
    geo_recommendations_full = geo_recommendations_filtered\
        .withColumn('processed_dttm',F.greatest("datetime_1","datetime_2"))\
        .join(geo_cities_tz,F.col('zone_id_1')==geo_cities_tz.id,'inner')\
        .withColumn('local_time',F.from_utc_timestamp(F.col('processed_dttm'),F.col('timezone')))\
        .selectExpr('user_id_1 as user_left','user_id_2 as user_right', 'processed_dttm', 'zone_id_1 as zone_id','local_time')

    geo_recommendations_full.write.mode("overwrite").parquet(f"{output_base_path}date={date}") # Записываем результат в analytics




if __name__ == "__main__":
        main()