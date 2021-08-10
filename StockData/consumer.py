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
import pydoop.hdfs as hdfs

consumer = KafkaConsumer('newTopic')
hdfs_path = 'hdfs://localhost:9000/StockData/stock_file.txt'

for message in consumer:
    values = message.value.decode('utf-8')
    with hdfs.open(hdfs_path, 'at') as f:
        f.write(f"{values}\n")