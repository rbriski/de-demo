from datetime import datetime, timedelta
import os.path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('demo',
    max_active_runs=1,
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)

create_discounts_by_quarter = """
    create or replace table {{ var.value.database }}.{{ var.value.spa_schema }}.discounts_by_fiscal_quarter as
    select c_start.fiscal_quarter, c_start.fiscal_year, count(*) as discounts
        from {{ var.value.database }}.{{ var.value.ods_schema }}.discount d 
        join {{ var.value.database }}.{{ var.value.ods_schema }}.calendar c_start on d.start_date=c_start.date
    group by 1,2
    order by 2,1
"""

with dag:
    start = DummyOperator(task_id='start', dag=dag)

    discount_by_quarter = SnowflakeOperator(
        task_id='discount_by_quarter',
        snowflake_conn_id='snowflake_default',
        sql=create_discounts_by_quarter,
        warehouse='ANALYTICS_LARGE',
        dag=dag
    )

    start >> discount_by_quarter