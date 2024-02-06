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
    output_base_path = sys.argv[4] #output_base_path = "/user/sushkos/data/analytics/geo/geo_user_activity/"
    checkpoint_dir = sys.argv[5] #checkpoint_dir = "/user/sushkos/data/checkpoints/geo/geo_user_activity"
    basePath = sys.argv[6] #basePath = "/user/sushkos/data/geo/events/"
    input_guides_path = sys.argv[7] #input_guides_path = "/user/sushkos/data/guides/"
    #studentusername = "sushkos"


    spark = SparkSession.builder \
                    .master("yarn") \
                    .appName("update_geo_user_activity_session") \
                    .getOrCreate()  
    
    spark.sparkContext.setCheckpointDir(checkpoint_dir) 


    gtu = geo_data_transformations.GeoTransformer()

    geo_cities = gu.get_cities(spark, input_guides_path,"geo.csv") #Читаем csv с городами

    geo_cities_tz = gu.get_timezones(spark, input_guides_path,"geo_tz.csv") #Читаем csv с таймзонами городов

    paths = gu.input_paths(basic_input_path,date, depth ,False, 'message') #Создаем список путей на чтение из prod

    geo_events = gu.get_events(spark, basePath , paths,False,'message') #Читаем events из prod

    #!hdfs dfs -rm -r "/user/sushkos/data/geo"
    #!hdfs dfs -rm -r "/user/sushkos/data/analytics"
    #!hdfs dfs -rm -r "/user/sushkos/data/checkpoints/geo/geo_zone_metrics"

    geo_min_distance = gtu.calculate_events_nearest_city(geo_events, geo_cities,False,True).checkpoint() #Рассчитываем город последнего сообщения пользователя

    #Считаем актуальным город
    act_city_window = Window.Window().partitionBy(['user_id']).orderBy(F.desc('date'))
    geo_act_city = geo_min_distance.withColumn("act_city_num",F.row_number().over(act_city_window))\
    .filter(F.col("act_city_num")==1).selectExpr("user_id",'city as act_city')

    #Считаем домашний город как самую свежую последовательность events >= 27 дней
    #Берем lag с текущей строкой по городу, ставим ноль при совпадении, размечаем напрерывные последовательности(новая последовательность начинается, если в is_prev_city появилась 1)
    #Считаем длину последовательности 
    prev_city_window = Window.Window().partitionBy('user_id').orderBy(F.desc('date')) 
    city_seq_window = Window.Window().partitionBy('user_id').orderBy(F.desc('date')).rowsBetween(Window.Window.unboundedPreceding, 0)
    geo_home_city = geo_min_distance\
                    .withColumn("is_prev_city",F.when(F.col('city')==F.lag('city').over(prev_city_window),0).otherwise(1))\
                    .withColumn("seq_group",F.sum('is_prev_city').over(city_seq_window))\
                    .select('user_id','city','seq_group','date')\
                    .groupBy(['user_id','city','seq_group']).agg(F.expr('count(seq_group) as seq_long'))\
                    .filter(F.col('seq_long')>=27)\
                    .groupBy(['user_id','city']).agg(F.min('seq_group'))\
                    .selectExpr('user_id','city as home_city')
    
    #                Ответ на комментарий из ревью - Выбор последовательности с самой новой датой заложен в значение seq_group. 
    #                Подробнее по строкам:
    #                prev_city_window = Window.Window().partitionBy('user_id').orderBy(F.desc('date'))
    #                .withColumn("is_prev_city",F.when(F.col('city')==F.lag('city').over(prev_city_window),0).otherwise(1))\ 
    #                1 присваиваем только 1 строке в последовательности(city != lag(city)), остальным строкам - 0

    #                .withColumn("seq_group",F.sum('is_prev_city').over(city_seq_window))\ # Суммируем в рамках последовательности значения "is_prev_city". 
    #                По сути работает как ранжирование, т.е. все строкам в рамках одной последовательности(от 1 до 1 в is_prev_city, с помощью форточки) присваевается одно значение, 
    #                которое инкрементируется на 1 в след. последовательности
    #                Т.к. окна prev_city_window и city_seq_window имеют order by date desc - последовательности ранжируются от самой новой к самой старой

    #                .groupBy(['user_id','city','seq_group']).agg(F.expr('count(seq_group) as seq_long'))\ группируем по номеру последовательности и считаем кол-во строк в ней

    #                .groupBy(['user_id','city']).agg(F.min('seq_group'))\ # Здесь мы просто берем минимальный номер последовательности, т.е. последовательность с самой новой датой.
  
    #Считаем уникальные посещенные города и кол-во путешествий
    travel_array_window = Window.Window().partitionBy('user_id').orderBy(F.asc('date'))

    geo_travel_array = geo_min_distance.select('user_id','date','city')\
        .withColumn('is_prev_city', F.when(F.col('city')==F.lag('city').over(travel_array_window),0).otherwise(1))\
        .filter(F.col('is_prev_city')==1)\
        .groupBy('user_id').agg(F.collect_list('city').alias('travel_array'))\
        .withColumn('travel_count', F.size('travel_array'))\
        .select('user_id','travel_count','travel_array')

    #Считаем local time
    geo_local_time = geo_min_distance.filter(F.col('datetime').isNotNull())\
        .groupBy('user_id','city').agg(F.max('datetime'))\
        .join(geo_cities_tz,['city'],'inner')\
        .withColumn('local_time',F.from_utc_timestamp(F.col('max(datetime)'),F.col('timezone')))\
        .select('user_id','local_time')

    #Соединяем в витрину
    geo_user_activity = geo_act_city\
        .join(geo_home_city,['user_id'],how='left')\
        .join(geo_travel_array,['user_id'],how='left')\
        .join(geo_local_time,['user_id'],how='left')

    geo_user_activity.write.mode("overwrite").parquet(f"{output_base_path}date={date}") # Записываем результат в analytics




if __name__ == "__main__":
        main()