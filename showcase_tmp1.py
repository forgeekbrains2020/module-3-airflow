from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'ayashin'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + 'pj_showcase_tmp1_etl',
    default_args=default_args,
    description='DWH ETL test tasks',
    schedule_interval="@yearly",
    max_active_runs=1,
)

fill_dim_billing_year = PostgresOperator(
    task_id="fill_dim_billing_year",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        insert into  ayashin.pj_dm_dim_billing_year(billing_year_key)
select distinct billing_year  from ayashin.pj_view_showcase prt
    left join ayashin.pj_dm_dim_billing_year prdby on prdby.billing_year_key = prt.billing_year
where prdby.billing_year_key  is null and  prt.billing_year ={{ execution_date.year }};
    """
)

