# python script to recieve bit coins data from from coin market cap......

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import pprint
from pywebhdfs.webhdfs import PyWebHdfsClient
import logging 
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

url="https://api.coinmarketcap.com/v1/ticker/?limit=0"
hdfs_dir = '/user/hammad/'


#getting cyptocurrency data. as much as possible, if we remove limit=0, we will get only 100 records.x
def recieveData(url):
	data = pd.read_json(url)
	data.index.name="id"
	#data.drop(data.columns[1], axis=1)

	pprint.pprint(data)

	writeToCsv(data)
def writeToCsv(data):
	logging.info (">>> writing data stream to data.csv")
	data.to_csv("data.csv", sep=',',header=False)
if __name__ == '__main__':
	recieveData(url)
#Hadoop operations.

def hadoop_hdfsOperations(hdfs_dir):

	hdfs = PyWebHdfsClient(host='', port='50070',
                       user_name='hdp')
	logging.info (">>> Creating dir using \'hdfs dfs -mkdir /user/hammad/ \'")
	hdfs.mkdir(hdfs_dir)
	copyFromLocaltoHadoop

def copyFromLocaltoHadoop():
	logging.debug (">>> Copying file from local storage to hdfs.")
	os.system("hdfs dfs -put data.csv /user/hammad/data.csv")