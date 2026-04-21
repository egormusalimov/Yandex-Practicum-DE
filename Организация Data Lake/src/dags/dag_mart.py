import airflow
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

import findspark

findspark.init()
findspark.find()

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2022, 1, 1),
}

dag_spark = DAG(
    dag_id = "sprint7_full_migration",
    default_args=default_args,
    schedule_interval=None,
)

dt = '{{ ds }}'

users = SparkSubmitOperator(
                        task_id='user_geography_task',
                        dag=dag_spark,
                        application ='/scripts/1_geography.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [f"{dt}",
                                            "/user/egormusali/data/geo",
                                            "/user/egormusali/data/geo.csv",
                                            "/user/egormusali/analytics/users_geo_mart"
                                            ],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )


zones = SparkSubmitOperator(
                        task_id='zones_geography_task',
                        dag=dag_spark,
                        application ='/scripts/2_zones.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [f"{dt}",
                                            "/user/egormusali/data/geo",
                                            "/user/egormusali/data/geo.csv",
                                            "/user/egormusali/analytics/users_geo_mart",
                                            "/user/egormusali/analytics/zones_mart",
                                            ],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )


recs = SparkSubmitOperator(
                        task_id='recommendations_mart_task',
                        dag=dag_spark,
                        application ='/scripts/3_mart.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [f"{dt}",
                                            "/user/egormusali/data/geo",
                                            "/user/egormusali/data/geo.csv",
                                            "/user/egormusali/analytics/users_geo_mart",
                                            "/user/egormusali/analytics/recommendations_mart",
                                            ],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )


users >> [zones, recs]