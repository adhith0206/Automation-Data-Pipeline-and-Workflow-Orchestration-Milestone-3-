import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'adhit',
    'start_date': dt.datetime(2025, 1, 8),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=600),
}


with DAG('P2M3_Adhit_Hikmatullah_DAG',
         default_args=default_args,
         schedule_interval='10-30/10 9 * * SAT',
         catchup=False,
         ) as dag:

    extract = BashOperator(task_id='extract', bash_command='sudo -u airflow python /opt/airflow/scripts/extract_rmt10.py')
    transform = BashOperator(task_id='transform', bash_command='sudo -u airflow python /opt/airflow/scripts/transform_rmt10.py')
    load = BashOperator(task_id='load', bash_command='sudo -u airflow python /opt/airflow/scripts/load_rmt10.py')
    

extract >> transform >> load