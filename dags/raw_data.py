# airflow related
from airflow import DAG
from airflow.operators.bash_operator import 

# email package
from airflow.operators.email_operator import EmailOperator

# other packages
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020 , 10, 9, 0, 0, 0),
    'email': ['pranit1617@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5
}

dag1 = DAG(
    dag_id='raw_data_dag',
    description='DAG for data pull and etl',
    schedule_interval='30 5 * * *',
    default_args=default_args)


task1 = BashOperator(
    task_id='raw_data_pull',
    bash_command='python ~/main_codes_new/data_pull_new.py',
    dag=dag1
    )

task2 = BashOperator(
    task_id='district_wise_daily_etl',
    bash_command='python ~/main_codes_new/district_wise_daily_etl.py',
    dag=dag1
    )

task3 = BashOperator(
    task_id='district_wise_daily_kpi',
    bash_command='python ~/main_codes_new/district_wise_daily_KPI.py',
    dag=dag1
    )

email_success = EmailOperator(
        task_id='send_email',
        to=['pranit.patil@fractal.ai'],
        subject='Airflow raw_data_dag Success',
        html_content=""" <h3>All tasks of raw_data_dag successfully executed</h3> """,
        dag=dag1
       )

task1>>task2>>task3>>email_success

if __name__ == "__main__" :
    dag1.cli()
