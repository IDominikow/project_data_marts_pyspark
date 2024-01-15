from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

date = "2022-06-21"
dpt = "30"
username = "sushkos"
basic_input_path = f"/user/{username}/data/geo/events/"
checkpoint_dir = f"/user/{username}/data/checkpoints/geo/"
basic_output_path = f"/user/{username}/data/analytics/geo/"
basePath = f"/user/{username}/data/geo/events/"
guides_path = f"/user/{username}/data/guides/"

basic_input_raw_path = "/user/master/data/geo/events/"
baseRawPath = "/user/master/data/geo/events/"
basic_output_raw_path = "/user/sushkos/data/geo/events/"

default_args = {
                                'owner': 'airflow',
                                'start_date':datetime(2024, 1, 13),
                                }

dag_spark = DAG(
                        dag_id = "project_geo_analysis_dag",
                        default_args=default_args,
                        schedule_interval='0 6 * * *',
                        )

load_data_to_prod = SparkSubmitOperator(
                        task_id='load_data_to_prod',
                        dag=dag_spark,
                        application ='/lessons/prod_data_loader.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [date, baseRawPath, basic_input_raw_path ,dpt, basic_output_raw_path]
                        )

calc_geo_user_activity = SparkSubmitOperator(
                        task_id='calc_geo_user_activity',
                        dag=dag_spark,
                        application ='/lessons/geo_user_activity.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [date, dpt , basic_input_path, f"{basic_output_path}geo_user_activity/"
                                             ,f"{checkpoint_dir}geo_user_activity/"
                                             ,basePath
                                             ,guides_path
                                             ]
                        )

calc_geo_zone_metrics = SparkSubmitOperator(
                        task_id='calc_geo_zone_metrics',
                        dag=dag_spark,
                        application ='/lessons/geo_zone_metrics.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [date, dpt , basic_input_path, f"{basic_output_path}geo_user_activity/"
                                             ,f"{checkpoint_dir}geo_user_activity/"
                                             ,basePath
                                             ,guides_path
                                             ]
                        )

calc_geo_recommendations = SparkSubmitOperator(
                        task_id='calc_geo_recommendations',
                        dag=dag_spark,
                        application ='/lessons/geo_recommendations.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [date, dpt , basic_input_path, f"{basic_output_path}geo_user_activity/"
                                             ,f"{checkpoint_dir}geo_user_activity/"
                                             ,basePath
                                             ,guides_path
                                             ]
                        )

clear_checkpoints = BashOperator(
    task_id="clear_geo_checkpoints",
    dag=dag_spark,
    bash_command=f"hdfs dfs -rm -r {checkpoint_dir}")


load_data_to_prod >> calc_geo_user_activity >> calc_geo_zone_metrics >> calc_geo_recommendations >> clear_checkpoints