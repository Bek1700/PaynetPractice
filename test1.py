

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import os
import io
import csv
import boto3
import requests
import datetime
import pandas as pd
import xml.etree.ElementTree as ET # Импортировали из библиотеки xml элемент tree и назвали его ET

from airflow.hooks.postgres_hook import PostgresHook # c помощью этого hook будем входить в наш Greenplan
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator



DEFAULT_ARGS = {
    'start_date': datetime.datetime(2020,1,1),
    'owner': 'Bekhzod',
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=2)
}

with DAG("bek_load_cbr_dynamic", # Меняем название нашего DAG
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov'],
          catchup=True                  # позволяет делать backfill
          ) as dag:
         
    def download_cbr_to_s3(**context):
        ds=context['ds']  # Получаем дату из контекста в формате 'YYYY-MM-DD'
        date_obj = datetime.datetime.strptime(ds, '%Y-%m-%d').strftime('%d/%m/%Y')  # Преобразуем дату в формат 'DD/MM/YYYY'
        print(f"Downloading CBR XML for date: {date_obj}")
        url = f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_obj}'  # Формируем URL с динамической датой
        response = requests.get(url, timeout=30)
        # проверяем что API ответил успешно
        if response.status_code != 200:
            raise Exception(f"CBR API returned {response.status_code}")
        response.encoding = 'windows-1251'  # Устанавливаем правильную кодировку
        xml_content = response.text
        parser = ET.XMLParser(encoding="UTF-8") # Создаем Parser формата UTF-8, натравливаем его на нужный файл ('/opt/airflow/include/bek/cbr.xml', parser=parser)
        root = ET.fromstring(xml_content)
        rows=[]
        for Valute in root.findall('Valute'):
            NumCode = Valute.find('NumCode').text
            CharCode = Valute.find('CharCode').text
            Nominal = Valute.find('Nominal').text
            Name = Valute.find('Name').text
            Value = Valute.find('Value').text
            rows.append({
                'Date': root.attrib['Date'],
                'ID': Valute.attrib['ID'],
                'NumCode': NumCode,
                'CharCode': CharCode,
                'Nominal': Nominal,
                'Name': Name,
                'Value': float(Value.replace(',', '.'))
            })
        df = pd.DataFrame(rows)
        buffer=io.BytesIO()
        df.to_parquet(buffer)
        buffer.seek(0)

        bucket_name = os.environ.get('MINIO_BUCKET', 'cbr-data')
        conn_id = os.environ.get('MINIO_CONN_ID', 'Minio-S3')

        s3_hook = S3Hook(aws_conn_id=conn_id)

        # Загружаем файл через S3Hook (просто и корректно для Minio/S3 Connection)
        s3_hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=f'cbr/{ds}.parquet',
            bucket_name=bucket_name,
            replace=True,
        )
        logging.info(f"Uploaded parquet to s3://{bucket_name}/cbr/{ds}.parquet")

    upload_to_minio_task=PythonOperator(
        task_id='download_cbr_to_s3',
        python_callable=download_cbr_to_s3,
        dag=dag
    )

download_cbr_to_s3