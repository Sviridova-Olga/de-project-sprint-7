from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/lessons"


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id="project7",
    default_args=default_args,
    schedule_interval=None,
)

spark_submit_start = SparkSubmitOperator(
    task_id='spark_submit_start',
    dag=dag_spark,
    application='/lessons/project7_init.py',
    conn_id='yarn_spark',
    application_args=["/user/sviridovao/data/geo_events", "/user/sviridovao/data/geo.csv",
                      "/user/sviridovao/data/events_with_city"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=4,
    executor_memory='4g'
)
spark_submit_mart1 = SparkSubmitOperator(
    task_id='spark_submit_task1',
    dag=dag_spark,
    application='/lessons/project7_mart1.py',
    conn_id='yarn_spark',
    application_args=["/user/sviridovao/data/events_with_city", "/user/sviridovao/data/analytics/mart1"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=4,
    executor_memory='4g'
)
spark_submit_mart2 = SparkSubmitOperator(
    task_id='spark_submit_task2',
    dag=dag_spark,
    application='/lessons/project7_mart2.py',
    conn_id='yarn_spark',
    application_args=["/user/sviridovao/data/events_with_city",
                      "/user/sviridovao/data/analytics/mart2"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=4,
    executor_memory='4g'
)
spark_submit_mart3 = SparkSubmitOperator(
    task_id='spark_submit_task3',
    dag=dag_spark,
    application='/lessons/project7_mart3.py',
    conn_id='yarn_spark',
    application_args=["/user/sviridovao/data/events_with_city",
                      "/user/sviridovao/data/analytics/mart3"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=4,
    executor_memory='4g'
)

spark_submit_start >> spark_submit_mart1 >> spark_submit_mart2 >> spark_submit_mart3
