from datetime import datetime
import os

from airflow.models.dag import DAG, TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

import pandas as pd
import requests
from sqlalchemy import create_engine

db = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS')
}

default_arg = {
    'owner': 'Bruno',
    'start_date': datetime(2022, 4, 27)
}

dag = DAG(dag_id='crypto_watcher', default_args=default_arg)


def build_dataframe(data):
    if data:
        return pd.DataFrame().from_records(data)


def save_on_database(dataframe):
    engine = create_engine(f'postgresql://{db.get("user")}:{db.get("password")}@localhost:5432/cryptodb')
    dataframe.to_sql('daily_crypto', engine, if_exists='replace')


def execute():
    try:
        response = requests.get('https://api2.binance.com/api/v3/ticker/24hr')
        if response != 200:
            raise Exception('Fail to fetch data from server')
        data = response.json()
        df = build_dataframe(data)
        save_on_database(df)
        return 'success_notify'
    except BaseException:
        return 'fail_notify'


init_pipeline = BashOperator(
    task_id='init_pipeline',
    bash_command='echo Initializing pipeline!',
    dag=dag
)

execute = BranchPythonOperator(
    task_id='execute',
    python_callable=execute,
    provide_context=True,
    dag=dag
)

success_notify = BashOperator(
    task_id='success_notify',
    bash_command='echo Pipeline finished!',
    dag=dag
)

fail_notify = BashOperator(
    task_id='fail_notify',
    bash_command='echo Pipeline failed!',
    dag=dag
)


init_pipeline >> execute >> [success_notify, fail_notify]
