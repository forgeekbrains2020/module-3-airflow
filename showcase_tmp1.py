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

fill_dim_legal_type = PostgresOperator(
    task_id="dim_legal_type",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        insert into  ayashin.pj_dm_dim_legal_type(legal_type_key)
select distinct legal_type as legal_type_key from ayashin.pj_view_showcase prt
    left join  ayashin.pj_dm_dim_legal_type on legal_type = legal_type_key
where legal_type_key is null and  prt.billing_year ={{ execution_date.year }};
    """
)


fill_dim_dim_district = PostgresOperator(
    task_id="fill_dim_dim_district",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        insert into  ayashin.pj_dm_dim_district(district_key)
select distinct district  from ayashin.pj_view_showcase prt
    left join ayashin.pj_dm_dim_district prdby on prdby.district_key = prt.district
where prdby.district_key  is null and  prt.billing_year ={{ execution_date.year }};
    """
)

fill_dim_billing_mode = PostgresOperator(
    task_id="fill_dim_billing_mode",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        insert into  ayashin.pj_dm_dim_billing_mode(billing_mode_key)
select distinct billing_mode  from ayashin.pj_view_showcase prt
    left join ayashin.pj_dm_dim_billing_mode prdby on prdby.billing_mode_key = prt.billing_mode
where prdby.billing_mode_key  is null and  prt.billing_year ={{ execution_date.year }};
    """
)

fill_dim_registration_year = PostgresOperator(
    task_id="fill_dim_registration_year",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        insert into  ayashin.pj_dm_dim_registration_year(registration_year_key)
select distinct registration_year  from ayashin.pj_view_showcase prt
    left join ayashin.pj_dm_dim_registration_year prdby on prdby.registration_year_key = prt.registration_year
where prdby.registration_year_key  is null and  prt.billing_year ={{ execution_date.year }};
    """
)

