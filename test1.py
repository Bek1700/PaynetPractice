"""
Складываем курс валют в GreenPlum (Меняем описание нашего дага)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import os
import csv
import boto3
import requests
import datetime
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

with DAG("bek_load_cbr_dynamic", # Меняем название нашего DAG
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov']
          ) as dag:
         
    def download_cbr_xml(**context):
        ds=context['ds']  # Получаем дату из контекста в формате 'YYYY-MM-DD'
        date_obj = datetime.datetime.strptime(ds, '%Y-%m-%d').strftime('%d/%m/%Y')  # Преобразуем дату в формат 'DD/MM/YYYY'
        print(f"Downloading CBR XML for date: {date_obj}")
        url = f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_obj}'  # Формируем URL с динамической датой
        response = requests.get(url)
        response.encoding = 'windows-1251'  # Устанавливаем правильную кодировку
        xml_content = response.text
        os.makedirs('/opt/airflow/include/bek', exist_ok=True)
        with open(f'/opt/airflow/include/bek/cbr_{date_obj}.xml', 'w', encoding='utf-8') as f:
            f.write(xml_content)

    download_cbr_xml_task = PythonOperator(
        task_id='download_cbr_xml', # Меняем название в нашем task
        python_callable=download_cbr_xml, # Вставляем нашу функцию
        dag=dag,
    )