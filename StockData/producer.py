'''
/**********************************************************************************
@Author: Amar Pawar
@Date: 2021-08-07
@Last Modified by: Amar Pawar
@Last Modified time: 2021-08-07
@Title : Producer program to store real time data on HDFS with kafka
/**********************************************************************************
'''
import csv
import requests
from kafka import KafkaProducer
import os
from json import dumps
from dotenv import load_dotenv
load_dotenv('.env')

bootstrap_servers = ['localhost:9092']
producer = KafkaProducer(bootstrap_servers = bootstrap_servers, value_serializer = lambda k:dumps(k).encode('utf-8'))
my_data = os.getenv("KEY")
# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=IBM&interval=1min&slice=year1month1&apikey={}'.format(my_data)

with requests.Session() as s:
    download = s.get(CSV_URL)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    for row in my_list:
        producer.send('newTopic', row)