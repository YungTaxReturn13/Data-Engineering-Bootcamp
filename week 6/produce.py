import csv
from json import dumps
from kafka import KafkaProducer
from time import sleep
import random 


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
    x = random.randrange(0,2)
    y = random.randrange(0,100)
    key = {"vendorId": str(x)}
    value = {"vendorId": x, "example_value": y}
    producer.send('homework', value=value, key=key)
    print("producing")
    sleep(1)
