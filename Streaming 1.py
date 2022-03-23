#Requirement: Save events in HDFS in text(json) format. Use "kafka" source and "file" sink. Set outputMode to "append".

#Source of Streaming data: meetup_rsvp

#Read data from meetup_rsvp and named it df
df = (spark.readStream
	.format('kafka')
	.option('kafka.bootstrap.servers', 'ip-172-31-91-232.ec2.internal:9092')
	.option('subscribe', 'meetup_rsvp')
	.option('startingOffsets', 'latest')
	.option('failOnDataLoss', 'false')
	.load())

#save as json files for every 5 seconds, checkpoint included
(df.writeStream
	.trigger(processingTime('5 seconds'))
	.format('json')
	.option('path', '/user/ken/Spark_Streaming/json')
	.option('checkpointLocation', '/user/ken/Spark_Streaming/checkpoint/checkpoint1')
	.outputMode('append')
	.start())
