import requests
import xml.etree.ElementTree as ET
import os
import pandas as pd
url = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req=01/11/2021'
response = requests.get(url)
if response.status_code != 200:
    raise Exception(f"CBR API returned {response.status_code}")
response.encoding = 'windows-1251'
xml_content = response.text
parser = ET.XMLParser(encoding="UTF-8")
root = ET.fromstring(xml_content, parser=parser)
for Valute in root.findall('Valute'):
    NumCode = Valute.find('NumCode').text
    print(NumCode,Valute)


