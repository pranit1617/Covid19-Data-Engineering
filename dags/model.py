# airflow related
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# email package
from airflow.operators.email_operator import EmailOperator

# other packages
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 10, 9, 0, 0, 0),
    'email': ['pranit1617@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5
}

dag2 = DAG(dag_id='ml_models_dag',
description='DAG for ml models',
schedule_interval='40 5 * * *',
default_args=default_args)

# the bash commad used in m_task1 activates conda environment and runs the model script in covid19 directory
m_task1 = BashOperator(
    task_id='model_data_pull',
    bash_command='cd ~/covid19/ && /home/.conda/envs/pymc3/bin/python run_bmc_pipeline_v2.py',
    dag=dag2
    )

m_task2 = BashOperator(
    task_id='combined_model_data',
    bash_command='python ~/Models/combined_model_data.py',
    dag=dag2
    )

m_task3 = BashOperator(
    task_id='model_rt_today',
    bash_command='python ~/Models/model_rt_today.py',
    dag=dag2
    )

email_success = EmailOperator(
        task_id='send_email',
        to=['pranit.patil@fractal.ai'],
        subject='Airflow ml_model_dag Success',
        html_content=""" <h3>All tasks of ml_model_dag successfully executed</h3> """,
        dag=dag2
       )

m_task1>>m_task2>>m_task3>>email_success

if __name__ == "__main__":
    dag2.cli()
