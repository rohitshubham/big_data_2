#!/usr/bin/python3

#Client 1 gets the streaming data as stringified json and hence can directly save it into the database(coredms)
# we just need to call the JSON 
import time
import logging
import paho.mqtt.client as mqtt
import traceback
import ast
import pymongo
import sys

custom_logging_format = '%(asctime)s : [%(levelname)s] - %(message)s'
logging.basicConfig(filename= "../../logs/company1/streamingestapp.log" , filemode="a", level= logging.INFO, format=custom_logging_format)

broker_url = "localhost"
broker_port = 1883
__connectionString = "mongodb://localhost:27017"
__databaseName = "mysimbdp-coredms"
total_messages = 0
last_reported_messages = 0 
avg_processing_time = 0
total_ingestion_data_size = 0
last_time_sent = 0


#topic-name = "big_data_" + truncate(sha256("company_1"), 5)
topic = "topic/big_data_c2dcc"
topic_reporting = "topic/big_data_reporting_c2dcc"

client = mqtt.Client()
client.connect(broker_url, broker_port)

def __connectToDatabase(collectionName):
    simpbdp_client = pymongo.MongoClient(__connectionString)
    getDatabase = simpbdp_client[__databaseName]
    return getDatabase[collectionName] 


def connectAndInsertRow(valueToBeStored):
    current_collection = __connectToDatabase("company_1_stream")
    current_collection.insert_one(valueToBeStored)


def start_ingestion(mqtt_string_data):
    my_dict = ast.literal_eval(mqtt_string_data)
    connectAndInsertRow(my_dict)


def on_connect(client, userdata, flags, rc):
    logging.info(f"Connected to {broker_url} with result code {str(rc)}.")
    client.subscribe(topic)
    logging.info(f"Subscribed to topic {topic} on {broker_url}")


def send_mqtt_requests(payload):
    client = mqtt.Client()
    client.connect(broker_url, broker_port, 60)
    client.publish(topic_reporting, str(payload))
    client.disconnect()

#reports the data after every 60 seconds 
def reporting_data():
    global last_reported_messages, avg_processing_time, total_messages, last_time_sent, total_ingestion_data_size
    if time.time() - last_time_sent > 60:
        send_mqtt_requests([avg_processing_time, total_messages-last_reported_messages, total_ingestion_data_size])
        total_ingestion_data_size = 0
        avg_processing_time = 0
        last_time_sent = time.time()        

def on_message(client, userdata, msg):
    try:
        global total_messages, avg_processing_time, total_ingestion_data_size
        logging.info(f"Received message for client_1. start the ingestion of data into the database.")
        start = time.time()
        start_ingestion(str(msg.payload.decode()))
        end = time.time()
        logging.info("DaTA SUCCESSFULLY INGESTED.")
        total_messages = total_messages + 1
        avg_processing_time = (avg_processing_time + (end-start))/2 # We calculate the avg after every message
        total_ingestion_data_size = total_ingestion_data_size + sys.getsizeof(str(msg.payload.decode()))
        reporting_data()
        logging.info(f"Reporting: Total messages ingested this session:{total_messages}")
        logging.info(f"Reporting: Size of this mesage was :{sys.getsizeof(str(msg.payload.decode()))}")
        logging.info(f"Reporting: Time taken for ingestion was {end-start}")
    except Exception as e:
        logging.error(f"Some error ocurred while ingesting the data. error is {traceback.format_exc()}")
client = mqtt.Client()
client.connect(broker_url, broker_port)

client.on_connect = on_connect
client.on_message = on_message

client.loop_forever()