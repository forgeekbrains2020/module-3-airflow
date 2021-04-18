from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'ayashin'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2019, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)

dds_user_hub = PostgresOperator(
    task_id="dds_user_hub",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.hub_user(select * from ayashin.view_hub_user_etl);
    """
)

dds_account_hub = PostgresOperator(
    task_id="dds_account_hub",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.hub_account(select * from ayashin.view_hub_account_etl);
    """
)

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

[dds_user_hub, dds_account_hub] >> all_hubs_loaded

dds_link_user_accounts = PostgresOperator(
    task_id="dds_link_user_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.link_user_accounts(select * from ayashin.view_link_user_accounts_etl);
    """
)

all_hubs_loaded >> dds_link_user_accounts

all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

dds_link_user_accounts >> all_links_loaded
