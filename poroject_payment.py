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
    USERNAME + '_poroject_payment_etl',
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


#source_ods_loaded = DummyOperator(task_id="all_source_ods_loaded", dag=dag)

fill_ods_payment  >> [dds_user_hub, dds_account_hub, dds_payment_hub,dds_billing_period_hub, dds_pay_doc_type_hub]
#fill_ods_payment  >> [dds_user_hub, dds_account_hub]

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

#[dds_user_hub, dds_account_hub] >> all_hubs_loaded
[dds_user_hub, dds_account_hub, dds_payment_hub, dds_billing_period_hub, dds_pay_doc_type_hub] >> all_hubs_loaded

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






all_hubs_loaded >> [dds_link_user_accounts, dds_link_pay_doc_type_payment, dds_link_payments_accounts_users, dds_link_payment_billing_period]
all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)
[dds_link_user_accounts, dds_link_pay_doc_type_payment, dds_link_payments_accounts_users, dds_link_payment_billing_period] >> all_links_loaded

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






all_links_loaded >> [dds_sat_user_details, dds_sat_payment]
all_sats_loaded = DummyOperator(task_id="all_sats_loaded", dag=dag)
[dds_sat_user_details, dds_sat_payment] >> all_sats_loaded
