#Requirement: 
  #Show how many events are received, display in a 2-minute tumbling window. 
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

#Create dataframe that counts number of events coming in every 2 minutes, set the delayThresold as 1 hour and display order from the oldest to the latest
df3 = (df.withWatermark("timestamp", "1 hours")
	.groupBy(window(col("timestamp"), "120 seconds"))
	.count()
	.orderBy(desc("window")))

#Show the result in console for every 1 minute, with checkpoint
(df3.writeStream
	.trigger(processingTime="60 seconds")
    .option("truncate", False)
    .option("checkpointLocation", "/spark_streaming/checkpoint_3")
    .queryName("record_count_per_2_minutes")
    .format("console")
    .outputMode("complete")
    .start()
    .awaitTermination())
