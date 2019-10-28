#!/usr/bin/python3

#This is the reporting service that listens to reporting channel and scales up the network
# we have the total number of instances of client1 and 2 and we do the scaling up and down using the  
# streaming-manager. This will scale up and down automatically and will be independent of the streaming manager
# 
from mysimbdpstreamingestmanager import run_command
import paho.mqtt.client as mqtt

#We define some threshold on the processing time ingestion data size to scale up or down
# This will ensure we decrease and increse the instances based on these thresholds
min_processing_time_threshold = 0.005
max_processing_time_threshold = 0.05

min_number_of_messages = 600000
max_number_of_messages = 6000000



topic_reporting = "topic/big_data_reporting"
broker_url = "localhost"
broker_port = 1883

total_client1_instances = 1
total_client2_instances = 1

#keeps track fo all the instances/process created by this file
client1Pid = []
client2Pid = []


def scaleup(client_name):
    pid = run_command(client_name, 0, "start")
    if client_name == "client1":
        client1Pid.append(pid)
    else:
        client2Pid.append(pid)

def scaledown(client_name):
    
    if client_name == "client1" and client1Pid: #if empty_list will evaluate as false.
        pid_to_remove = client1Pid.pop(0)
    if client_name == "client2" and client2Pid: #if empty_list will evaluate as false.
        pid_to_remove = client2Pid.pop(0)
    if pid_to_remove:
        run_command(client_name, int(pid_to_remove), "stop")


    
#dies some basic checks and then calls the method to spawn a new process or kill a process
def scale_up_or_down(client_name, avg_processing_time, number_of_messages):
    if avg_processing_time > max_processing_time_threshold:
        scaleup(client_name)
    if avg_processing_time < min_processing_time_threshold:
        scaledown(client_name)
    if number_of_messages > max_number_of_messages: # we need to scale down in this case as processing rate is too high
        scaledown(client_name)
    if number_of_messages < min_number_of_messages:
        scaleup(client_name) 
    
    

def on_connect(client, userdata, flags, rc):
    client.subscribe(topic_reporting)

def on_message(client, userdata, msg):
    message_list = eval(str(msg.payload.decode()))
    client_name = message_list[0] 
    avg_processing_time = message_list[1]
    number_of_messages = message_list[2]
    total_ingestion_data_size = message_list[3]
    scale_up_or_down(client_name, avg_processing_time, number_of_messages)


client = mqtt.Client()
client.connect(broker_url, broker_port)

client.on_connect = on_connect
client.on_message = on_message

client.loop_forever()