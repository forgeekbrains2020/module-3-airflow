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
    USERNAME + 'tmp_project_showcase_etl',
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
select distinct billing_year as billing_year_key  from ayashin.pj_view_showcase prt
    left join ayashin.pj_dm_dim_billing_year prdby on prdby.billing_year_key = prt.billing_year
where prdby.billing_year_key  is null and  prt.billing_year ={{ execution_date.year }};
    """
)

fill_dim_legal_type = PostgresOperator(
    task_id="fill_dim_legal_type",
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
select distinct district as district_key  from ayashin.pj_view_showcase prt
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
select distinct billing_mode as billing_mode_key from ayashin.pj_view_showcase prt
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
select distinct registration_year as registration_year_key from ayashin.pj_view_showcase prt
    left join ayashin.pj_dm_dim_registration_year prdby on prdby.registration_year_key = prt.registration_year
where prdby.registration_year_key  is null and  prt.billing_year ={{ execution_date.year }};
    """
)

all_dim_loaded = DummyOperator(task_id="all_dim_loaded", dag=dag)

[fill_dim_billing_year , fill_dim_legal_type , fill_dim_dim_district, fill_dim_billing_mode, fill_dim_billing_mode, fill_dim_registration_year ]  >> all_dim_loaded

fill_dim_fct = PostgresOperator(
    task_id="fill_dim_fct",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
    insert into ayashin.tmp_pj_dm_dim_fct (billing_year_id, legal_type_id, district_id, billing_mode_id, registration_year_id, is_vip, payment_sum, traffic_amount)
        select biy.id, lt.id, d.id, bim.id, ry.id, is_vip, raw.payment_sum, raw.traffic_amount
from ayashin.pj_view_showcase raw
join ayashin.pj_dm_dim_billing_year biy on raw.billing_year = biy.billing_year_key
join ayashin.pj_dm_dim_billing_mode bim on raw.billing_mode = bim.billing_mode_key
join ayashin.pj_dm_dim_legal_type lt on raw.legal_type = lt.legal_type_key
join ayashin.pj_dm_dim_district d on raw.district = d.district_key
join ayashin.pj_dm_dim_registration_year ry on raw.registration_year = ry.registration_year_key
where billing_year_key ={{ execution_date.year }};
    """
)

all_dim_loaded >> fill_dim_fct


