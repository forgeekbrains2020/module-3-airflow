from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'ayashin'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_data_lake_etl',
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
)

ods_billing = DataProcHiveOperator(
    task_id='ods_billing',
    dag=dag,
    query="""
        insert overwrite table ayashin.ods_billing partition (year='{{ execution_date.year }}') 
        select  user_id,from_unixtime(unix_timestamp(billing_period , 'yyyy-MM')),service, tariff, cast(sum as decimal(19,2)),timestamp(created_at, 'yyyy-MM-dd') from ayashin.stg_billing where year(created_at) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_billing_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)


ods_payment = DataProcHiveOperator(
    task_id='ods_payment',
    dag=dag,
    query="""
        insert overwrite table ayashin.ods_payment partition (year='{{ execution_date.year }}') 
        select user_id, pay_doc_type, pay_doc_num, account, phone, from_unixtime(unix_timestamp(billing_period , 'yyyy-MM')), timestamp(pay_date , 'yyyy-MM-dd'),    cast(sum as decimal(19,2)) from ayashin.stg_payment where year(pay_date) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_payment_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_issue = DataProcHiveOperator(
    task_id='ods_issue',
    dag=dag,
    query="""
        insert overwrite table ayashin.ods_ods_issue partition (year='{{ execution_date.year }}') 
        select * from ayashin.stg_issue where year(start_time) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_issue_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_traffic = DataProcHiveOperator(
    task_id='ods_traffic',
    dag=dag,
    query="""
        insert overwrite table ayashin.ods_traffic partition (year='{{ execution_date.year }}') 
        select user_id,  from_unixtime(cast(`timestamp`/1000 as int)), device_id, device_ip_addr, bytes_sent, bytes_received from ayashin.stg_traffic where year(from_unixtime(cast(`timestamp`/1000 as int))) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

dm_traffic_by_user = DataProcHiveOperator(
    task_id='dm_traffic_by_user',
    dag=dag,
    query="""
        insert overwrite table ayashin.dm_traffic_by_user partition (year='{{ execution_date.year }}') 
        select user_id, max(bytes_received), min(bytes_received), avg(bytes_received) from ayashin.ods_traffic where year =  {{ execution_date.year }} GROUP BY user_id;
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_trafficby_user_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)
ods_traffic >> dm_traffic_by_user
