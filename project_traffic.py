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
    USERNAME + '_project_traffic_etl',
    default_args=default_args,
    description='DWH ETL test tasks',
    schedule_interval="@yearly",
    max_active_runs=1,
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





dds_hub_traffic_ip = PostgresOperator(
    task_id="dds_hub_traffic_ip",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_ip(select * from ayashin.pj_view_hub_ip_etl);
    """
)


dds_hub_traffic_device = PostgresOperator(
    task_id="dds_hub_traffic_device",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_device(select * from ayashin.pj_view_hub_device_etl);
    """
)

dds_hub_traffic_user = PostgresOperator(
    task_id="dds_hub_traffic_user",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT into ayashin.pj_dds_hub_user(select * from ayashin.pj_view_hub_traffic_user_etl);
    """
)



fill_ods_traffic  >> [dds_hub_traffic_ip, dds_hub_traffic_device, dds_hub_traffic_user ]
#fill_ods_payment  >> [dds_user_hub, dds_account_hub]

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

#[dds_user_hub, dds_account_hub] >> all_hubs_loaded
[ dds_hub_traffic_ip, dds_hub_traffic_device, dds_hub_traffic_user] >> all_hubs_loaded







dds_link_traffic_user_device = PostgresOperator(
    task_id="dds_link_traffic_user_device",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
INSERT into ayashin.pj_dds_link_traffic_user_device(select * from ayashin.pj_view_link_traffic_user_device_etl);
    """
)



all_hubs_loaded >> [ dds_link_traffic_user_device]
all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)
[dds_link_traffic_user_device] >> all_links_loaded



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



all_links_loaded >> [ dds_sat_traffic_user_device]
all_sats_loaded = DummyOperator(task_id="all_sats_loaded", dag=dag)
[ dds_sat_traffic_user_device] >> all_sats_loaded
