#Requirement: Save events in HDFS in text(json) format. Use "kafka" source and "file" sink. Set outputMode to "append".

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

#Cast the datatype of column 'value' to string and save the new dataframe as df1
df1=df.select(col('value').cast('string'))
df1.printSchema()


#Save as json files for every 5 seconds, checkpoint included
(df1.writeStream
	.trigger(processingTime('5 seconds'))
	.format('json')
	.option('path', '/user/ken/Spark_Streaming/json')
	.option('checkpointLocation', '/user/ken/Spark_Streaming/checkpoint/checkpoint1')
	.outputMode('append')
	.start())

#Additional way - changing format to text can save only the values of the dataframe
(df1.writeStream
	.trigger(processingTime('5 seconds'))
	.format('text')
	.option('path', '/user/ken/Spark_Streaming/json')
	.option('checkpointLocation', '/user/ken/Spark_Streaming/checkpoint/checkpoint1')
	.outputMode('append')
	.start())
