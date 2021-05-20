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
    USERNAME + '_sat_test_etl',
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
    select * from( SELECT a.user_pk,
           a.user_hashdiff,
           a.phone,
           a.effective_from,
           a.load_date,
           a.record_source,
           row_number()
           OVER (PARTITION BY a.user_hashdiff ORDER BY a.effective_from) AS row_number
    FROM ayashin.pj_ods_v_payment a where  a.load_date =  '{{ execution_date }}'::TIMESTAMP) h
    --'2013-01-01 00:00:00.000000 +00:00'::TIMESTAMP) h-- and user_pk = 'bf0377d5a3968bac5aac7a10dfff1b86')
    where h.row_number = 1

    ),
-- Выбираем имеющиеся данные из сателита, но только те которые младше данных которые пришли
--
     update_records AS (
         SELECT a.user_pk,
                a.user_hashdiff,
                a.phone,
                a.effective_from,
                a.load_date,
                a.record_source
         --FROM ayashin.pj_dds_sat_users_details a
         FROM ayashin.pj_dds_sat_users_details a
                  JOIN source_data as b
                      ON a.user_pk = b.user_pk
         where a.load_date <=  b.load_date--(select max(load_date) from source_data)
     )-- select * from update_records
                     ,
-- Из имеющихся данных сателита, что младше, тех даных что пришли, выберем самую свежую?
     latest_records AS (
         SELECT *
         FROM (SELECT c.user_pk,
                      c.user_hashdiff,
                      c.load_date,
                      c.effective_from,
                      CASE
                          WHEN (rank() OVER (PARTITION BY c.user_pk ORDER BY c.load_date DESC)) = 1 THEN 'Y'::text
                          ELSE 'N'::text
                          END AS latest
               FROM update_records c) s
         WHERE s.latest = 'Y'::text
     )  --select * from latest_records --where  user_pk ='bf0377d5a3968bac5aac7a10dfff1b86'
     ,
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
         WHERE latest_records.user_hashdiff IS NULL -- and latest_records.user_pk ='bf0377d5a3968bac5aac7a10dfff1b86'
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
select * from ( SELECT a.payments_pk,
           a.payment_hashdif,
           a.pay_date,
           a.sum,
           a.effective_from,
           a.load_date,
           a.record_source,
           row_number()
           OVER (PARTITION BY a.payment_hashdif ORDER BY a.effective_from) AS row_number
    FROM ayashin.pj_ods_v_payment a where  a.load_date =  '{{ execution_date }}'::TIMESTAMP) h
    where h.row_number = 1
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



dds_sat_traffic_user_device = PostgresOperator(
    task_id="dds_sat_traffic_user_device",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.pj_dds_sat_traffic_user_device (traffic_pk, traffic_hashdiff, bytes_sent, bytes_received, effective_from, load_date, record_source)
WITH source_data AS (
    SELECT a.traffic_pk,
           a.traffic_hashdiff,
           a.bytes_sent,
           a.bytes_received,
           a.effective_from,
           a.load_date,
           a.record_source
    --FROM ayashin.pj_ods_v_payment a where a.load_date =  '2014-01-01 00:00:00.000000 +00:00'
    FROM ayashin.pj_ods_v_traffic a where  a.load_date =  '{{ execution_date }}'::TIMESTAMP
),
update_records AS (
         SELECT a.traffic_pk,
                a.traffic_hashdiff,
                a.bytes_sent,
                a.bytes_received,
                a.effective_from,
                a.load_date,
                a.record_source
         FROM ayashin.pj_dds_sat_traffic_user_device a
                  JOIN source_data as b
                      ON a.traffic_pk = b.traffic_pk
    where a.load_date <= (select max(load_date) from source_data)
     ),
     latest_records AS (
         SELECT *
         FROM (SELECT c.traffic_pk,
                      c.traffic_hashdiff,
                      c.bytes_sent,
                      c.bytes_received,
                      c.effective_from,
                      c.load_date,
                      c.record_source
                      ,
                      CASE
                          WHEN (rank() OVER (PARTITION BY c.traffic_pk ORDER BY c.load_date DESC)) = 1 THEN 'Y'::text
                          ELSE 'N'::text
                          END AS latest
               FROM update_records c) s
         WHERE s.latest = 'Y'::text
     ),
     records_to_insert AS (
         SELECT e.traffic_pk,
                e.traffic_hashdiff,
                e.bytes_sent,
                e.bytes_received,
                e.effective_from,
                e.load_date,
                e.record_source
         FROM source_data e
                  LEFT JOIN latest_records ON latest_records.traffic_hashdiff = e.traffic_hashdiff and latest_records.traffic_pk = e.traffic_pk
         WHERE latest_records.traffic_hashdiff IS NULL
     )
SELECT * FROM records_to_insert
    """
)

dds_sat_user_details >> dds_sat_payment >> dds_sat_traffic_user_device
