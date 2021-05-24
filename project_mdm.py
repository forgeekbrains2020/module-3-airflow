from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'ayashin'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2010, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_project_mdm_etl',
    default_args=default_args,
    description='DWH ETL test tasks',
    schedule_interval="@yearly",
    max_active_runs=1,
)


fill_ods_mdm = PostgresOperator(
    task_id="fill_ods_mdm",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
    delete  from ayashin.pj_ods_mdm where extract (year from registered_at)={{ execution_date.year }};
    insert into ayashin.pj_ods_mdm
    select *, '{{ execution_date}}'::TIMESTAMP as load_date from mdm.user where extract (year from registered_at)={{ execution_date.year }};
    """
)

dds_hub_mdm_billing_mode = PostgresOperator(
    task_id="dds_hub_mdm_billing_mode",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_billing_mode(select * from ayashin.pj_view_hub_billing_mode_etl);
    """
)

dds_hub_mdm_district = PostgresOperator(
    task_id="dds_hub_mdm_district",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_district(select * from ayashin.pj_view_hub_district_etl);
    """
)

dds_hub_mdm_legal_type = PostgresOperator(
    task_id="dds_hub_mdm_legal_type",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_legal_type(select * from ayashin.pj_view_hub_legal_type_etl);
    """
)

dds_hub_mdm_user_status = PostgresOperator(
    task_id="dds_hub_mdm_user_status",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_user_status(select * from ayashin.pj_view_hub_user_status_etl);
    """
)

dds_hub_mdm_user = PostgresOperator(
    task_id="dds_hub_mdm_user",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_user(select * from ayashin.pj_view_hub_mdm_user_etl);
    """
)



source_mdm_loaded = DummyOperator(task_id="all_source_ods_loaded", dag=dag)

fill_ods_mdm  >> source_mdm_loaded
source_mdm_loaded >> [dds_hub_mdm_billing_mode, dds_hub_mdm_district, dds_hub_mdm_legal_type, dds_hub_mdm_user_status, dds_hub_mdm_user]
#fill_ods_payment  >> [dds_user_hub, dds_account_hub]

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

#[dds_user_hub, dds_account_hub] >> all_hubs_loaded
[dds_hub_mdm_billing_mode, dds_hub_mdm_district, dds_hub_mdm_legal_type, dds_hub_mdm_user_status, dds_hub_mdm_user] >> all_hubs_loaded




dds_link_user_registration = PostgresOperator(
    task_id="dds_link_user_registration",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.pj_dds_link_user_registration(select * from ayashin.pj_view_link_user_registration_etl);
    """
)




all_hubs_loaded >> [dds_link_user_registration]
all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)
[dds_link_user_registration] >> all_links_loaded






