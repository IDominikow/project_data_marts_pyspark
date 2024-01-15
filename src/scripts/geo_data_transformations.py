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
import datetime
import geo_utils as gu
import math


class GeoTransformer:

    #def __init__(self, date):
    #    self.date = date

    def calculate_events_nearest_city(self,geo_events, geo_cities, appendRegistrationEvent = False, onlyMessage = False):
    
        pi_rad = math.pi/180
        if onlyMessage: #Если True - расчитываем только по последним сообщениям для 1й витрины

            #Переводим градусы в радианы
            geo_events_rad = geo_events\
                .withColumn("lat_rad",(geo_events.lat)*pi_rad)\
                .withColumn("lon_rad",(geo_events.lon)*pi_rad)\
                .selectExpr("event.message_from as user_id",'date as date','event.datetime as datetime', "event.message_id as message_id","lat_rad", "lon_rad")

            #Делаем cross join с таблицей городов и расчитываем расстояние между городом и координатами event
            geo_events_cross = geo_events_rad.crossJoin(geo_cities)\
            .withColumn("sphere_distance",\
                        2*6371*F.asin(
                        (F.sqrt((F.pow(F.sin((F.col('city_lat_rad') - F.col('lat_rad'))/F.lit(2)),2))\
                        +F.cos('lat_rad')*F.cos('city_lat_rad')\
                        *(F.pow(F.sin((F.col('city_lon_rad') - F.col('lon_rad'))/F.lit(2)),2))                            
                        ))))\
            .select(["user_id",'date','datetime',"message_id","city","sphere_distance"])

            min_distance_window = Window.Window().partitionBy(['user_id','date','message_id']).orderBy(F.asc('sphere_distance'))

        else:    #Если False - считаем по всем событиям

            #Переводим градусы в радианы
            geo_events_rad = geo_events.selectExpr('event.message_from as user_id','event.datetime as datetime','event.reaction_from as react_user'\
                                ,'event.user as usr', 'lat', 'lon', 'date', 'event_type')\
                .withColumn('user_id',F.concat_ws('','usr','user_id','react_user').cast('long'))\
                .withColumn('event_lat_rad', (F.regexp_replace('lat',',','.').cast("double"))*pi_rad)\
                .withColumn('event_lon_rad', (F.regexp_replace('lon',',','.').cast("double"))*pi_rad)\
                .drop('lat').drop('lon').drop('react_user').drop('usr').checkpoint()
            
            
            if appendRegistrationEvent:#Если True - добавляем в DataFrame событие "ragistration"

                geo_registration = geo_events_rad\
                    .withColumn("date_row_num",F.row_number()\
                                .over(Window.Window().partitionBy('user_id').orderBy(F.asc('date')))\
                            ).filter(F.col('date_row_num')==1)\
                    .withColumn('event_type',F.lit('registration'))\
                    .drop('date_row_num')

                geo_full_events = geo_events_rad.unionByName(geo_registration)
            else: 
                geo_full_events = geo_events_rad

            #Находим координаты последнего события для заполнения событий без координат
            geo_last_rad = geo_full_events.filter(F.col('event_lat_rad').isNotNull())\
                .withColumn("date_row_num",F.row_number()\
                            .over(Window.Window().partitionBy('user_id').orderBy(F.desc('date')))\
                        ).filter(F.col('date_row_num')==1)\
                .selectExpr('user_id','event_lat_rad as last_rad','event_lon_rad as last_lon')
            
            #Добавляем координаты последнему событию
            geo_full_events_rad = geo_full_events.filter(F.col('event_lat_rad').isNull()).join(geo_last_rad,['user_id'],how = 'left')\
                .selectExpr('user_id','datetime','date','event_type','last_rad as event_lat_rad','last_lon as event_lon_rad')\
                .unionByName(geo_full_events.filter(F.col('event_lat_rad').isNotNull()))\
                .withColumn('row_num_temp',F.monotonically_increasing_id())
            
            #Делаем cross join с таблицей городов и расчитываем расстояние между городом и координатами event
            geo_events_cross = geo_full_events_rad.crossJoin(geo_cities)\
                .withColumn("sphere_distance",\
                            2*6371*F.asin(
                            (F.sqrt((F.pow(F.sin((F.col('city_lat_rad') - F.col('event_lat_rad'))/F.lit(2)),2))\
                            +F.cos('event_lat_rad')*F.cos('city_lat_rad')\
                            *(F.pow(F.sin((F.col('city_lon_rad') - F.col('event_lon_rad'))/F.lit(2)),2))                            
                            ))))\
                .select(["user_id",'date','datetime','event_type','row_num_temp',"zone_id","sphere_distance"])
        
            
            min_distance_window = Window.Window().partitionBy(['user_id','date','row_num_temp']).orderBy(F.asc('sphere_distance'))
        
        #Выбираем ближайший город для каждого события
        return geo_events_cross.withColumn("row_num",F.row_number().over(min_distance_window))\
            .filter(F.col('row_num')==1)
    


    def calculate_user_pairs_by_conditions(self,geo_events):
        #Берем уникальные user_id из всех доступных столбцов
        geo_users = geo_events\
            .selectExpr('event.message_from as user_id').filter(F.col('user_id').isNotNull()).distinct()\
            .union(geo_events.selectExpr('event.message_to as user_id').filter(F.col('user_id').isNotNull()).distinct())\
            .union(geo_events.selectExpr('event.reaction_from as user_id').filter(F.col('user_id').isNotNull()).distinct())\
            .union(geo_events.selectExpr('event.user as user_id').filter(F.col('user_id').isNotNull()).distinct())\
            .distinct().select(F.col('user_id').cast('long')).checkpoint()
        
        #делаем cross join для получения пар юзеров, удаляем дубли с собой
        geo_users_pairs_full = geo_users\
            .crossJoin(geo_users.selectExpr('user_id as user_id_2'))\
            .filter(F.col('user_id')!=F.col('user_id_2')).checkpoint()
        
        #Находим пары юзеров с сообщениями
        geo_user_pairs_message = geo_events.filter(F.col('event_type')=='message')\
            .selectExpr('event.message_from as user_id_1','event.message_to as user_id_2').distinct()
        
        #Удаляем пары с сообщениями из общего пула
        geo_pairs_wout_message = geo_users_pairs_full.subtract(geo_user_pairs_message)
        
        #Находим юзеров и каналы, на которые они подписаны
        geo_user_channels = geo_events.filter(F.col('event_type')=='subscription')\
            .select(F.col('event.subscription_channel').alias('channel_id')\
                    ,F.col('event.user').cast('long').alias('chan_user_id'))\
            .groupBy('chan_user_id').agg(F.collect_set('channel_id').alias('channel_array'))
        
        #Соединяем общий список с каналами для каждого юзера в паре
        #Находим пересечечения в списках каналов юзеров в паре
        #Удаляем пары без пересечения
        geo_users_wchannel_wout_message = geo_pairs_wout_message\
            .join(geo_user_channels,geo_pairs_wout_message.user_id_1==geo_user_channels.chan_user_id,how='inner')\
            .selectExpr('user_id_1','user_id_2','channel_array as channel_arr_1')\
            .join(geo_user_channels,geo_pairs_wout_message.user_id_2==geo_user_channels.chan_user_id,how='inner')\
            .withColumn('arr_intersect',F.array_intersect(F.col('channel_arr_1'),F.col('channel_array')))\
            .filter(F.size(F.col('arr_intersect'))!=0)\
            .select('user_id_1','user_id_2')
        
        #Удаляем взаимные дубликаты в паре
        return geo_users_wchannel_wout_message\
                .withColumn('mix_1',F.concat_ws('_','user_id_1','user_id_2'))\
                .withColumn('mix_2',F.concat_ws('_','user_id_2','user_id_1'))\
                .dropDuplicates(['mix_1', 'mix_2'])
    