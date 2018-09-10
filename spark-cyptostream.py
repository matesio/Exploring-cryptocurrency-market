from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder\
		.appName("Cryptocurrency analysis")\
		.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
coinSchema = StructType([StructField("index",StringType(),True),\
					StructField("24h_volume_usd", StringType(),True),\
					StructField("available_supply", StringType(),True),\
					StructField("id",StringType(), True),\
					StructField("last_updated",StringType(),True),\
					StructField("market_cap_usd",StringType(),True),\
					StructField("max_supply",StringType(),True),\
					StructField("name",StringType(),True),\
					StructField("percent_change_1h",StringType(),True),\
					StructField("percent_change_24h",StringType(),True),\
					StructField("percent_change_7d",StringType(),True),\
					StructField("price_btc",StringType(),True),\
					StructField("price_usd",StringType(),True),\
					StructField("rank",StringType(),True),\
					StructField("symbol",StringType(),True),\
					StructField("total_supply",StringType(),True)])
fileStreamDF = spark.readStream\
				.option ("header", "true")\
				.option ("maxFilesPerTrigger", 1)\
				.schema(coinSchema)\
				.csv("./data/") 


average_price_usb_btc = fileStreamDF.groupBy("id")\
									.agg({"price_usd" : "avg"})\
									.withColumnRenamed("avg(price_usd)","average_price_usd")\
									.orderBy("average_price_usd",ascending=False)

average_cap_usb_btc = fileStreamDF.groupBy("id")\
									.agg({"market_cap_usd" : "avg"})\
									.withColumnRenamed("avg(market_cap_usd)","market_cap_usd")\
									.orderBy("market_cap_usd",ascending=False)

query = average_price_usb_btc.writeStream\
							.outputMode("complete")\
							.format("console")\
							.option("truncate","false")\
							.start()\
							.awaitTermination()
