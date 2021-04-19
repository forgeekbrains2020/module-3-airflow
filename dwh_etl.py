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

fill_ods_payment = PostgresOperator(
    task_id="fill_ods_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""insert into ayashin.ods_payment
    select *  from ayashin.stg_payment where extract (year from pay_date)={{ execution_date.year }};
    """
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

dds_payment_hub = PostgresOperator(
    task_id="dds_payment_hub",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.hub_payment(select * from ayashin.view_hub_payment_etl);
    """
)

dds_pay_doc_type_hub = PostgresOperator(
    task_id="dds_pay_doc_type_hub",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.hub_pay_doc_type(select * from ayashin.view_hub_pay_doc_type_etl);
    """
)

fill_ods_payment  >> [dds_user_hub, dds_account_hub, dds_payment_hub, dds_pay_doc_type_hub]

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

[dds_user_hub, dds_account_hub, dds_payment_hub, dds_pay_doc_type_hub] >> all_hubs_loaded

dds_link_pay_doc_type_payment = PostgresOperator(
    task_id="dds_link_pay_doc_type_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.link_pay_doc_type_payment(select * from ayashin.view_link_pay_doc_type_payment_etl);
    """
)
dds_link_user_accounts = PostgresOperator(
    task_id="dds_link_user_accounts",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.link_payments_accounts_users(select * from ayashin.view_link_payments_accounts_users_etl);
    """
)


dds_link_payments_accounts_users = PostgresOperator(
    task_id="dds_link_payments_accounts_users",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.link_user_accounts(select * from ayashin.view_link_user_accounts_etl);
    """
)


dds_link_payment_billing_period = PostgresOperator(
    task_id="dds_link_payment_billing_period",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.link_payment_billing_period(select * from ayashin.view_link_payment_billing_period_etl);
    """
)



all_hubs_loaded >> [dds_link_user_accounts, dds_link_pay_doc_type_payment, dds_link_payments_accounts_users, dds_link_payment_billing_period]
all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)
[dds_link_user_accounts, dds_link_pay_doc_type_payment, dds_link_payments_accounts_users, dds_link_payment_billing_period] >> all_links_loaded

dds_sat_user_details = PostgresOperator(
    task_id="dds_sat_user_details",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.sat_users_details (select * from ayashin.view_sat_user_details_etl);
    """
)

dds_sat_payment = PostgresOperator(
    task_id="dds_sat_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.sat_payment (select * from ayashin.view_sat_payment_etl);
    """
)

all_links_loaded >> [dds_sat_user_details, dds_sat_payment]
all_sats_loaded = DummyOperator(task_id="all_sats_loaded", dag=dag)
[dds_sat_user_details, dds_sat_payment] >> all_sats_loaded

ods_clear = PostgresOperator(
    task_id="ods_clear",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
       delete  from ayashin.ods_payment where user_id>0;
    """
)
all_sats_loaded  >> ods_clear
