# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#defining DAG arguments
default_args = {
    'owner': 'Marin',
    'start_date': days_ago(0),
    'email': ['marin@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_web_log',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

# Define the tasks in the DAG
extract_data = BashOperator(
    task_id='extract_data',
    bash_command='cat /home/project/airflow/dags/accesslog.txt | awk \'{print $1}\' > /home/project/airflow/dags/extracted_data.txt',
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='grep -v "198.46.149.143" /home/project/airflow/dags/extracted_data.txt > /home/project/airflow/dags/transformed_data.txt',
    dag=dag
)

load_data = BashOperator(
    task_id='load_data',
    bash_command='tar -cvf /home/project/airflow/dags/weblog.tar /home/project/airflow/dags/transformed_data.txt',
    dag=dag
)

# task pipeline
extract_data >> transform_data >> load_data
