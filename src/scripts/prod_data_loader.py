import sys
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
import findspark
findspark.init()
findspark.find()
from pyspark.sql import SparkSession
import geo_utils as gu
 
def main():
    date = sys.argv[1]
    basePath = sys.argv[2]
    base_input_path = sys.argv[3]
    depth = sys.argv[4]
    base_output_path = sys.argv[5]

    spark = SparkSession.builder \
                    .master("yarn") \
                    .appName("load_geo_data_session") \
                    .getOrCreate()
    
    paths = gu.input_paths(base_input_path, date, depth, 'full')

    #Чтение из master
    geo_read = gu.get_events(spark, basePath, paths) # чтение из raw
    #base_input_path = "/user/master/data/geo/events/"

    gu.load_events_to_prod(geo_read, base_output_path) #запись в prod
    #base_output_path "/user/sushkos/data/geo/events"

    
if __name__ == "__main__":
        main()