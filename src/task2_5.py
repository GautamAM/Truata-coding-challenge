from airflow import DAG
from airflow.operators import DummyOperator,PythonOperator
from datetime import datetime, timedelta

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
)

dag = DAG('simple', default_args=default_args)
t1 = DummyOperator(
    task_id='testairflow',
    bash_command='python C:\\Users\Lansrod\Desktop\\truata project\pyspark_introduction\src\\task1_1.py',
    dag=dag)
t2 = DummyOperator(
    task_id='testairflow',
    bash_command='python C:\\Users\Lansrod\Desktop\\truata project\pyspark_introduction\src\\task1_2.py;'
                 'python C:\\Users\Lansrod\Desktop\\truata project\pyspark_introduction\src\\task1_3.py;',
    dag=dag)
t3 = DummyOperator(
    task_id='testairflow',
    bash_command='python C:\\Users\Lansrod\Desktop\\truata project\pyspark_introduction\src\\task2_1.py;'
                 'python C:\\Users\Lansrod\Desktop\\truata project\pyspark_introduction\src\\task2_2.py;',
                  'python C:\\Users\Lansrod\Desktop\\truata project\pyspark_introduction\src\\task2_3.py;',
    dag=dag)
