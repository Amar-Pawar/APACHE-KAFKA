'''
/**********************************************************************************
@Author: Amar Pawar
@Date: 2021-08-07
@Last Modified by: Amar Pawar
@Last Modified time: 2021-08-07
@Title : Consumer program to store real time data on HDFS with kafka
/**********************************************************************************
'''
from kafka import KafkaConsumer
import sys

consumer = KafkaConsumer('newTopic')
sys.stdout = open('stock_data.txt','w')
for message in consumer:
    values = message.value.decode('utf-8')
    print(values)
sys.stdout.close()