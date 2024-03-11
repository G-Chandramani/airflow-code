from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
##This used to load a script from a different directory
##In our use case the script to load data into snowflake is located in a subfolder inside the dags folder----

import sys
sys.path.append('/home/airflow/airflow-code/dags')
from source_load.data_load import run_script

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 28),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='Netflix_Data_Analytics',
    default_args=default_args,
    description='This dag runs data analytics on top of netflix datasets',
    schedule_interval=timedelta(days=1),
)

credits_sensor = S3KeySensor(
    task_id='credits_rawfile_sensor',
    poke_interval=60 * 5,
    timeout=60 * 60 * 24 * 7,
    bucket_key='raw_files/credits.csv',
    wildcard_match=True,
    bucket_name='netflixdataanalytics',
    aws_conn_id='aws_default',
    dag=dag
)

titles_sensor = S3KeySensor(
    task_id='titles_rawfile_sensor',
    poke_interval=60 * 5,
    timeout=60 * 60 * 24 * 7,
    bucket_key='raw_files/titles.csv',
    wildcard_match=True,
    bucket_name='netflixdataanalytics',
    aws_conn_id='aws_default',
    dag=dag
)

load_data_snowflake = PythonOperator(task_id='Load_Data_Snowflake'
    ,python_callable=run_script, 
    dag=dag)
    
run_stage_model = BashOperator(
    task_id='run_staging_models',
  #  bash_command='source /home/airflow/dbt-env/bin/activate && /home/airflow/dbt-env/bin/dbt run --model tag:"DIMENSION" --project-dir /home/airflow/dbt-code --profile Netflix --target dev',
   # bash_command='source /home/airflow/dbt-env/bin/activate && /home/airflow/dbt-env/bin/dbt run --model tag:"DIMENSION" --project-dir /home/airflow/dbt-code --profile Netflix --target dev >> /home/airflow/dbt_run.log 2>&1',
    bash_command='echo "Activating virtual environment" && source /home/airflow/dbt-env/bin/activate && echo "Running dbt command" && /home/airflow/dbt-env/bin/dbt run --model tag:"DIMENSION" --project-dir /home/airflow --profile Netflix --target dev',
    dag=dag
)

run_fact_dim_models = BashOperator(
        task_id='run_fact_dim_models',
  #  bash_command='/usr/local/bin/dbt run --model tag:"FACT" --project-dir /home/airflow/dbt-code/ --profiles-dir /home/airflow/dbt-code/.dbt/ --profile Netflix --target prod',
    bash_command='echo "Activating virtual environment" && source /home/airflow/dbt-env/bin/activate && echo "Running dbt command" && /home/airflow/dbt-env/bin/dbt run --model tag:"FACT" --project-dir /home/airflow --profile Netflix --target prod',
    dag=dag
)

run_test_cases = BashOperator(
 task_id='run_test_cases',
#bash_command='/usr/local/bin/dbt run --model tag:"TEST" --project-dir /home/airflow/dbt-code/ --profiles-dir /home/airflow/dbt-code/.dbt/ --profile Netflix --target prod',
 bash_command='echo "Activating virtual environment" && source /home/airflow/dbt-env/bin/activate && echo "Running dbt command" && /home/airflow/dbt-env/bin/dbt run --model tag:"TEST" --project-dir /home/airflow --profile Netflix --target prod',
 dag=dag
)

start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> credits_sensor >> titles_sensor >> load_data_snowflake  >> run_stage_model >> run_fact_dim_models >> run_test_cases >> end_task
#final check