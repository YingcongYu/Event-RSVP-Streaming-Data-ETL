#Requirement: Save events in HDFS in parquet format with schema. Use "kafka" source and "file" sink. Set outputMode to "append".

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

#Cast the datatype of column 'value' to string and save the new dataframe as df2
df2 = df.select(col("value").cast("string"))
df2.printSchema()

#Assign schema for each column under 'value'
#Save as parquet files WITH SCHEMA for every 1 minute, with checkpoint
(df2.select(get_json_object('value', '$.city').alias('city'),
		get_json_object('value', '$.rsvp_id').alias('rsvp_id'),
		get_json_object('value', '$.company').alias('company'),
		get_json_object('value', '$.name').alias('name'),
		get_json_object('value', '$.state').alias('state'))
	.writeStream.trigger(processingTime = '1 minute')
	.option('path', '/user/ken/Spark_Streaming/parquet')
	.option('checkpointLocation', '/user/ken/Spark_Streaming/checkpoint/checkpoint2')
	.outputMode('append')
	.start())

#Check the files
check = spark.read.parquet('/user/ken/Spark_Streaming/parquet')
check.show()
