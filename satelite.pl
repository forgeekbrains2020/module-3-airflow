from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'ayashin'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2020, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_tmp2_project_etl',
    default_args=default_args,
    description='DWH ETL test tasks',
    schedule_interval="@yearly",
    max_active_runs=1,
)


dds_sat_user_details = PostgresOperator(
    task_id="dds_sat_user_details",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.pj_dds_sat_users_details (user_pk, user_hashdiff, phone, effective_from, load_date, record_source)
WITH source_data AS (
    SELECT a.user_pk,
           a.user_hashdiff,
           a.phone,
           a.effective_from,
           a.load_date,
           a.record_source
    FROM ayashin.pj_ods_v_payment a where a.load_date =  '{{ execution_date }}'::TIMESTAMP
),
     update_records AS (
         SELECT a.user_pk,
                a.user_hashdiff,
                a.phone,
                a.effective_from,
                a.load_date,
                a.record_source
         FROM ayashin.pj_dds_sat_users_details a
                  JOIN source_data as b
                      ON a.user_pk = b.user_pk
         where a.load_date <= (select max(load_date) from source_data)
     ),
     latest_records AS (
         SELECT *
         FROM (SELECT c.user_pk,
                      c.user_hashdiff,
                      c.load_date,
                      CASE
                          WHEN (rank() OVER (PARTITION BY c.user_pk ORDER BY c.load_date DESC)) = 1 THEN 'Y'::text
                          ELSE 'N'::text
                          END AS latest
               FROM update_records c) s
         WHERE s.latest = 'Y'::text
     ),
     records_to_insert AS (
         SELECT distinct
                e.user_pk,
                e.user_hashdiff,
                e.phone,
                e.effective_from,
                e.load_date,
                e.record_source
         FROM source_data e
                  LEFT JOIN latest_records ON latest_records.user_hashdiff = e.user_hashdiff and latest_records.user_pk =e.user_pk
         WHERE latest_records.user_hashdiff IS NULL
     )
SELECT * FROM records_to_insert
"""
)

dds_sat_payment = PostgresOperator(
    task_id="dds_sat_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.pj_dds_sat_payment (payments_pk, payment_hashdif, pay_date, sum, effective_from, load_date, record_source)
WITH source_data AS (
    SELECT a.payments_pk,
           a.payment_hashdif,
           a.pay_date,
           a.sum,
           a.effective_from,
           a.load_date,
           a.record_source
    
    FROM ayashin.pj_ods_v_payment a where  a.load_date =  '{{ execution_date }}'::TIMESTAMP
),
update_records AS (
         SELECT a.payments_pk,
                a.payment_hashdif,
                a.pay_date,
                a.sum,
                a.effective_from,
                a.load_date,
                a.record_source
         FROM ayashin.pj_dds_sat_payment a
                  JOIN source_data as b
                      ON a.payments_pk = b.payments_pk
    where a.load_date <= (select max(load_date) from source_data)
     ),
     latest_records AS (
         SELECT *
         FROM (SELECT c.payments_pk,
                      c.payment_hashdif,
                      c.pay_date,
                      c.effective_from,
                      c.load_date,
                      c.record_source,
                      CASE
                          WHEN (rank() OVER (PARTITION BY c.payments_pk ORDER BY c.load_date DESC)) = 1 THEN 'Y'::text
                          ELSE 'N'::text
                          END AS latest
               FROM update_records c) s
         WHERE s.latest = 'Y'::text
     ),
     records_to_insert AS (
         SELECT e.payments_pk,
                e.payment_hashdif,
                e.pay_date,
                e.sum,
                e.effective_from,
                e.load_date,
                e.record_source
         FROM source_data e
                  LEFT JOIN latest_records ON latest_records.payment_hashdif = e.payment_hashdif and latest_records.payments_pk = e.payments_pk
         WHERE latest_records.payment_hashdif IS NULL
     )
SELECT * FROM records_to_insert
    """
)

all_links_loaded >> [dds_sat_user_details, dds_sat_payment]
all_sats_loaded = DummyOperator(task_id="all_sats_loaded", dag=dag)
[dds_sat_user_details, dds_sat_payment] >> all_sats_loaded

'''ods_clear = PostgresOperator(
    task_id="ods_clear",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
       delete  from ayashin.ods_payment where user_id>0;
    """
)
all_sats_loaded  >> ods_clear
'''
