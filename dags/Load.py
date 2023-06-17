from datetime import datetime
from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task,dag
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
from airflow.models.baseoperator import chain


date_transformed_data = Dataset("/tmp/date_tranformed.csv")

# A DAG represents a workflow, a collection of tasks
@dag(dag_id="data_load", start_date=datetime(2023, 6, 11), schedule=[date_transformed_data])
def etl_process():

    @task()
    def show_data():
        pd.read_csv(date_transformed_data.uri)
        print(pd)


    show_data()

etl_process()