'''
/**********************************************************************************
@Author: Amar Pawar
@Date: 2021-08-07
@Last Modified by: Amar Pawar
@Last Modified time: 2021-08-07
@Title : Program to store real time data on HDFS with kafka and subprocess
/**********************************************************************************
'''
import subprocess

def run_cmd(args_list):
    print("Running: {}'".format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output=proc.communicate()
    return output

if __name__=="__main__":
    cmd = run_cmd(['hadoop', 'fs','-put', '/home/ubuntu/Documents/hadoop_practice/stock_data.txt','/stock_data'])