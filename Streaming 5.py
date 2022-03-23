#Requirement: 
  #Use impala to create a KUDU table. Do dataframe transformation to extract information and write to the KUDU table. Use "kafka" source and "kudu" sink.

#Source of Streaming data: meetup_rsvp

#Create a KUDU table in Impala
create table if not exists rsvp_db.rsvp_kudu_ken(
rsvp_id         bigint primary key,
city    string,
company     string,
name string,
state   string)
PARTITION BY HASH PARTITIONS 2 STORED AS KUDU;

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

#Cast the column 'value' to datatype string and get all of its items
df5 = (df.withColumn('value', col('value').cast('string'))
	.select(get_json_object('value', '$.city').alias('city'),
		get_json_object('value', '$.rsvp_id').alias('rsvp_id'),
		get_json_object('value', '$.company').alias('company'),
		get_json_object('value', '$.name').alias('name'),
		get_json_object('value', '$.state').alias('state')))

#Cast the column 'rsvp_id' to datatype bigint
df5 = df5.withColumn('rsvp_id', col('rsvp_id').cast('bigint'))

#Save to the KUDU table
(df5.writeStream.trigger(processingTime = '30 seconds')
	.option('checkpointLocation', '/user/ken/Spark_Streaming/checkpoint/checkpoint5')
	.format('kudu')
	.option('kudu.master', 'ip-172-31-89-172.ec2.internal,ip-172-31-86-198.ec2.internal,ip-172-31-93-228.ec2.internal')
	.option('kudu.table', 'impala::rsvp_db.rsvp_kudu_ken')
	.option('kudu.operation', 'upsert')
	.start())
