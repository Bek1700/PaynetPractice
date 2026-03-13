"""
Складываем курс валют в GreenPlum (Меняем описание нашего дага)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import os
import csv
import boto3
import xml.etree.ElementTree as ET # Импортировали из библиотеки xml элемент tree и назвали его ET

from airflow.hooks.postgres_hook import PostgresHook # c помощью этого hook будем входить в наш Greenplan
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'Bekhzod',
    'poke_interval': 600
}

# TODO: вынести url, файлы с xml и csv в константу

dag = DAG("bek_load_cbr", # Меняем название нашего DAG
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov']
          )


load_cbr_xml_script = '''
mkdir -p /opt/airflow/include/bek
curl https://www.cbr.ru/scripts/XML_daily.asp?date_req=01/11/2021 | iconv -f Windows-1251 -t UTF-8 > /opt/airflow/include/bek/cbr.xml
'''
load_cbr_xml = BashOperator(
    task_id='load_cbr_xml', # Меняем название в нашем task
    bash_command=load_cbr_xml_script, # Вставляем нашу команду, так как она не помещается в единую строку, то используем форматер url 
    dag=dag
)


def xml_to_csv(**context):
    xml_path = '/opt/airflow/include/bek/cbr.xml'
    csv_path = '/opt/airflow/include/bek/cbr.csv'

    if not os.path.exists(xml_path):
        raise FileNotFoundError(f"XML file not found: {xml_path}")

    with open(xml_path, "rb") as f:
        tree = ET.parse(f)

    root = tree.getroot()

    # Ожидаем структуру <ValCurs Date="..." name="..."> <Valute ID="..."> ... </Valute> ... </ValCurs>
    rows = []
    for valute in root.findall('Valute'):
        char_code = valute.findtext('CharCode')
        num_code = valute.findtext('NumCode')
        nominal = valute.findtext('Nominal')
        name = valute.findtext('Name')
        value = valute.findtext('Value')

        rows.append({
            'NumCode': num_code,
            'CharCode': char_code,
            'Nominal': nominal,
            'Name': name,
            'Value': value,
        })

    with open(csv_path, 'w', encoding='utf-8', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['NumCode', 'CharCode', 'Nominal', 'Name', 'Value'])
        writer.writeheader()
        writer.writerows(rows)

    logging.info('Converted XML to CSV: %s', csv_path)
    return csv_path


def upload_to_minio(**context):
    csv_path = context['ti'].xcom_pull(task_ids='xml_to_csv')
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f'CSV file not found: {csv_path}')

    bucket_name = os.environ.get('MINIO_BUCKET', 'cbr-data')
    object_key = os.environ.get('MINIO_OBJECT', 'cbr.csv')
    conn_id = os.environ.get('MINIO_CONN_ID', 'Minio-S3')

    s3_hook = S3Hook(aws_conn_id=conn_id)

    # Создаст bucket, если не существует (для S3Hook можно выполнить через client)
    client = s3_hook.get_conn()
    s3_resource = s3_hook.get_resource_type('s3')
    bucket = s3_resource.Bucket(bucket_name)
    if bucket.creation_date is None:
        client.create_bucket(Bucket=bucket_name)
        logging.info('Created S3 bucket %s', bucket_name)

    s3_hook.load_file(
        filename=csv_path,
        key=object_key,
        bucket_name=bucket_name,
        replace=True,
    )
    logging.info('Uploaded CSV %s to s3://%s/%s', csv_path, bucket_name, object_key)


xml_to_csv_task = PythonOperator(
    task_id='xml_to_csv',
    python_callable=xml_to_csv,
    dag=dag,
)

upload_to_minio_task = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_to_minio,
    dag=dag,
)

load_cbr_xml >> xml_to_csv_task >> upload_to_minio_task
