from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'ayashin'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + 'test_my_dwh_etl',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)

fill_ods_payment = PostgresOperator(
    task_id="fill_ods_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
    delete  from ayashin.test_ods_payment where extract (year from pay_date)={{ execution_date.year }};
    insert into ayashin.test_ods_payment
    select *, '{{ execution_date}}'::TIMESTAMP as load_date from ayashin.stg_payment where extract (year from pay_date)={{ execution_date.year }};
    """
)

dds_user_hub = PostgresOperator(
    task_id="dds_user_hub",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.test_hub_user(select * from ayashin.test_view_hub_user_etl);
    """
)
