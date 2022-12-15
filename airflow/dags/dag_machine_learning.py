# import libraries
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.utils.dates import days_ago 



# function to get parent-dag most recent execution result
def most_recent_spark(dt):
    dag_runs = DagRun.find(dag_id="etl_spark")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date


# DAG Definition
default_args = {
	'owner': 'Admin'
}

with DAG(
    "machine_learning",
    start_date = days_ago(1),
    schedule_interval = None,
    default_args = default_args
) as dag:

    # Start job
    job_start = DummyOperator(
        task_id = "job_start"
        )


    # Get dag etl_spark result that had succeed earlier
    spark_task = ExternalTaskSensor(
        task_id = 'spark_task',
        external_dag_id = 'etl_spark',
        external_task_id = 'job_finish',
        execution_date_fn = most_recent_spark,
        allowed_states = ['success']
        )

    # Run machine learning script
    data_cleaning = BashOperator(
    	task_id = 'data_cleaning',
    	bash_command='python3 /home/agnes/Documents/digital_skola/Project/final_project/machine_learning/Home_Credit_Default_Risk_data_cleaning.py'
        )

    # Run machine learning script
    machine_learning = BashOperator(
    	task_id = 'machine_learning',
    	bash_command='python3 /home/agnes/Documents/digital_skola/Project/final_project/machine_learning/Home_Credit_Default_Risk_machine_learning.py'
        )

    # Finish job
    job_finish = DummyOperator(
        task_id = "job_finish"
        )


    # Orchestration
    (
        job_start
        >> spark_task
        >> data_cleaning
        >> machine_learning
        >> job_finish
    )
