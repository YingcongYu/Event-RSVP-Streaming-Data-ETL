#Requirement: 
  #Show how many events are received for each country, display it in a sliding window (set windowDuration to 3 minutes and slideDuration to 1 minutes). 
  #Show result at 1-minute interval. Use "kafka" source and "console" sink. Set outputMode to "complete".

#Source of Streaming data: meetup_rsvp

#Import nessary packages
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *

#Read data from meetup_rsvp and named it df
df = (spark.readStream
	.format('kafka')
	.option('kafka.bootstrap.servers', 'ip-172-31-91-232.ec2.internal:9092')
	.option('subscribe', 'meetup_rsvp')
	.option('startingOffsets', 'latest')
	.option('failOnDataLoss', 'false')
	.load())

#Check schema
df.printSchema()

#Only select two columns and cast the 'value' to datatype string
df4 = df.select(col('value').cast('string'), col('timestamp'))
df4.printSchema()

#Get the schema from previous practice(streaming 1)
schema_path = '/user/ken/Spark_Streaming/json'
historical = spark.read.json(schema_path)
rsvp_schema = historical.schema

#Get contents from column 'value' and only select the contents and timestamp, named as df4_2
df4_1 = df4.select(from_json(col('value'), rsvp_schema).alias('record'),col('timestamp'))
df4_2 = df4_1.select(col('record.state'), col('timestamp'))
df4_2.printSchema()

#Create a dataframe that counts number of events for every 1 minute and last for 3 minutes
#set the delayThresold as 20 minutes and display order from the oldest to the latest
final_df = (df4_2.withWatermark('timestamp', '20 minutes')
	.groupBy(window(col('timestamp'), '3 minutes', "1 minutes'),col('state'))
	.count()
	.sort(desc('window'), col('state')))

#Show the result in console for every 1 minute, with checkpoint
(final_df.writeStream
	.trigger(processingTime = "60 seconds")
	.option("truncate", False)
	.option("checkpointLocation", chkpoint_path)
	.queryName("window_count_by_country")
	.format("console")
	.outputMode("complete")
	.start()
	.awaitTermination())
