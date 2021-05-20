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
    USERNAME + '_prjall2_source_etl',
    default_args=default_args,
    description='DWH ETL test tasks',
    schedule_interval="@yearly",
    max_active_runs=1,
)

fill_ods_payment = PostgresOperator(
    task_id="fill_ods_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        delete  from ayashin.pj_ods_payment where extract (year from pay_date)={{ execution_date.year }};
        insert into ayashin.pj_ods_payment
        select *, '{{ execution_date}}'::TIMESTAMP as load_date from ayashin.pj_stg_payment where extract (year from pay_date)='{{ execution_date.year }}';
    """
)

fill_ods_traffic = PostgresOperator(
    task_id="fill_ods_traffic",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
    delete  from ayashin.pj_ods_traffic where extract (year from timestamp)={{ execution_date.year }};
    insert into ayashin.pj_ods_traffic
    select user_id, to_timestamp(cast(timestamp/1000 as int))::timestamp at time zone 'UTC' as timestamp,device_id, device_ip_addr, bytes_sent, bytes_received, '{{ execution_date}}'::TIMESTAMP as load_date from ayashin.pj_stg_traffic where extract (year from (to_timestamp(cast("timestamp"/1000 as int))::timestamp at time zone 'UTC'))='{{ execution_date.year }}';
    """
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

dds_user_hub = PostgresOperator(
    task_id="dds_user_hub",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_user(select * from ayashin.pj_view_hub_user_etl);
    """
)

dds_account_hub = PostgresOperator(
    task_id="dds_account_hub",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_account(select * from ayashin.pj_view_hub_account_etl);
    """
)

dds_payment_hub = PostgresOperator(
    task_id="dds_payment_hub",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_payment(select * from ayashin.pj_view_hub_payment_etl);
    """
)

dds_billing_period_hub = PostgresOperator(
    task_id="dds_billing_period_hub",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_billing_period(select * from ayashin.pj_view_hub_billing_period_etl);
    """
)

dds_pay_doc_type_hub = PostgresOperator(
    task_id="dds_pay_doc_type_hub",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_pay_doc_type(select * from ayashin.pj_view_hub_pay_doc_type_etl);
    """
)

dds_hub_billing_mode = PostgresOperator(
    task_id="dds_hub_mdm_billing_mode",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_billing_mode(select * from ayashin.pj_view_hub_billing_mode_etl);
    """
)

dds_hub_district = PostgresOperator(
    task_id="dds_hub_mdm_district",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_district(select * from ayashin.pj_view_hub_district_etl);
    """
)

dds_hub_legal_type = PostgresOperator(
    task_id="dds_hub_mdm_legal_type",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_legal_type(select * from ayashin.pj_view_hub_legal_type_etl);
    """
)

dds_hub_user_status = PostgresOperator(
    task_id="dds_hub_mdm_user_status",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_user_status(select * from ayashin.pj_view_hub_user_status_etl);
    """
)

dds_hub_ip = PostgresOperator(
    task_id="dds_hub_traffic_ip",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_ip(select * from ayashin.pj_view_hub_ip_etl);
    """
)


dds_hub_device = PostgresOperator(
    task_id="dds_hub_traffic_device",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_device(select * from ayashin.pj_view_hub_device_etl);
    """
)

all_source_ods_loaded = DummyOperator(task_id="all_source_ods_loaded", dag=dag)

[fill_ods_traffic , fill_ods_mdm , fill_ods_payment]  >> all_source_ods_loaded
all_source_ods_loaded >> [dds_user_hub, dds_account_hub, dds_payment_hub,dds_billing_period_hub, dds_pay_doc_type_hub, dds_hub_billing_mode, dds_hub_mdm_district, dds_hub_mdm_legal_type, dds_hub_mdm_user_status, dds_hub_traffi_ip, dds_hub_traffic_device ]
#fill_ods_payment  >> [dds_user_hub, dds_account_hub]

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

#[dds_user_hub, dds_account_hub] >> all_hubs_loaded
[dds_user_hub, dds_account_hub, dds_payment_hub, dds_billing_period_hub, dds_pay_doc_type_hub, dds_hub_mdm_billing_mode, dds_hub_mdm_district, dds_hub_mdm_legal_type, dds_hub_mdm_user_status, dds_hub_traffic_ip, dds_hub_traffic_device] >> all_hubs_loaded

dds_link_pay_doc_type_payment = PostgresOperator(
    task_id="dds_link_pay_doc_type_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.pj_dds_link_pay_doc_type_payment(select * from ayashin.pj_view_link_pay_doc_type_payment_etl);
    """
)
dds_link_user_accounts = PostgresOperator(
    task_id="dds_link_user_accounts",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.pj_dds_link_payments_accounts_users(select * from ayashin.pj_view_link_payments_accounts_users_etl);
    """
)


dds_link_payments_accounts_users = PostgresOperator(
    task_id="dds_link_payments_accounts_users",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.pj_dds_link_user_accounts(select * from ayashin.pj_view_link_user_accounts_etl);
    """
)


dds_link_payment_billing_period = PostgresOperator(
    task_id="dds_link_payment_billing_period",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.pj_dds_link_payment_billing_period(select * from ayashin.pj_view_link_payment_billing_period_etl);
    """
)



dds_link_user_registration = PostgresOperator(
    task_id="dds_link_user_registration",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.pj_dds_link_user_registration(select * from ayashin.pj_view_link_user_registration_etl);
    """
)

dds_link_traffic_user_device = PostgresOperator(
    task_id="dds_link_traffic_user_device",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.pj_dds_link_traffic_user_device(select * from ayashin.pj_view_link_traffic_user_device_etl);
    """
)



all_hubs_loaded >> [dds_link_user_accounts, dds_link_pay_doc_type_payment, dds_link_payments_accounts_users, dds_link_payment_billing_period, dds_link_user_registration, dds_link_traffic_user_device]
all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)
[dds_link_user_accounts, dds_link_pay_doc_type_payment, dds_link_payments_accounts_users, dds_link_payment_billing_period, dds_link_user_registration, dds_link_traffic_user_device] >> all_links_loaded

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



all_links_loaded >> [dds_sat_user_details, dds_sat_payment, dds_sat_traffic_user_device]
all_sats_loaded = DummyOperator(task_id="all_sats_loaded", dag=dag)
[dds_sat_user_details, dds_sat_payment, dds_sat_traffic_user_device] >> all_sats_loaded
