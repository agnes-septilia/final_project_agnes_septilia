from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago 


# DAG Definition
default_args = {
	'owner': 'Admin',
}

with DAG(
    "etl_spark",
    start_date = days_ago(1),
    schedule_interval = None,
    default_args = default_args
) as dag:

    # Start job
    job_start = DummyOperator(
        task_id = "job_start"
        )

    # ETL data from csv to mysql
    csv_to_mysql = BashOperator(
    	task_id = 'csv_to_mysql',
    	bash_command='python3 /home/agnes/Documents/digital_skola/Project/final_project/spark/csv_to_mysql.py'
        )
    
    # ETL data from mysql to postgres
    mysql_to_postgres = BashOperator(
    	task_id = 'mysql_to_postgres',
    	bash_command='python3 /home/agnes/Documents/digital_skola/Project/final_project/spark/mysql_to_postgres.py'
        )


    # Finish job
    job_finish = DummyOperator(
        task_id = "job_finish"
        )


    # Orchestration
    (
        job_start
        >> csv_to_mysql
        >> mysql_to_postgres
        >> job_finish
    )
