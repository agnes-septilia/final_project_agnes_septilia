from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.dates import days_ago 


# DAG Definition
default_args = {
	'owner': 'Admin',
}

with DAG(
    "fact_daily",
    start_date = days_ago(1),
    schedule_interval = '@daily',
    default_args = default_args
) as dag:

    # Start job
    job_start = DummyOperator(
        task_id = "job_start"
        )
    
    # Create dependencies from dag dim
    dim_tables = BaseSensorOperator(
        task_id='dim_tables',
        external_dag_id='dim_table'
        )

    # Run fact table total_per_state sql script
    create_fact_total_per_state = PostgresOperator(
        task_id = 'create_fact_total_per_state',
        postgres_conn_id = "airflow_final_project",
        sql = "/home/agnes/Documents/digital_skola/Project/final_project/fact_table/fact_total_per_state.sql"
    )
 
    # Run fact table daily_avg_currency sql script
    create_fact_daily_avg_currency = PostgresOperator(
        task_id = 'create_fact_daily_avg_currency',
        postgres_conn_id = "airflow_final_project",
        sql = "/home/agnes/Documents/digital_skola/Project/final_project/fact_table/fact_daily_avg_currency.sql"
    )
  
    # Finish job
    job_finish = DummyOperator(
        task_id = "job_finish"
        )


    # Orchestration
    (
        job_start
        >> dim_tables
        >> create_fact_total_per_state
        >> create_fact_daily_avg_currency
        >> job_finish
    )
