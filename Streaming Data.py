from faker import Faker
from random import seed, randint, choice
import json
import time
from kafka import KafkaProducer

#Create a producer and name the topic 'meetup_rsvp'
producer = KafkaProducer(bootstrap_servers = 'ip-172-31-91-232.ec2.internal:9092,ip-172-31-89-11.ec2.internal:9092')
topic = 'meetup_rsvp'

#Create a list of states
state_list = ['VA', 'MD', 'CA', 'DC', 'DE']

#Set the upper boundary for ids
rsvp_id_upper = 9223372036854775808

#Set send out time of one message for every 2 second
message_interval = 2

#Use Faker to generate randomness
fake = Faker()
seed(1)

#Assign data into a dictionary
while True:
    dict = {}
    dict['rsvp_id'] = randint(0, rsvp_id_upper)
    dict['name'] = fake.name()
    dict['company'] = fake.company()
    dict['city'] = fake.city()
    dict['state'] = choice(state_list)
    
#Convert the dictionary to json string & send the string to the producer
    json_string = json.dumps(dict)
    producer.send(topic, key=None, value=json_string)
    time.sleep(message_interval)

#Create a consumer to get values from meetup_rsvp
from kafka import KafkaConsumer
consumer = KafkaConsumer('meetup_rsvp', bootstrap_servers='ip-172-31-91-232.ec2.internal:9092,ip-172-31-89-11.ec2.internal:9092')
for message in consumer:
    print (message.value)
