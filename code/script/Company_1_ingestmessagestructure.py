
import time 
import paho.mqtt.client as mqtt

#topic-name = "big_data_" + truncate(sha256("company_1"), 5)
topic = "topic/big_data_c2dcc"
broker_url = "localhost"
broker_port = 1883

def getFormattedData(message):
    try:
        data = message.split(",")
        myDict = {  "id" : data[0],
                "name" : data[1],
                "host_id" : data[2],
                "host_name" : data[3],
                "neighbourhood_group" : data[4],
                "neighbourhood" : data[5],
                "latitude" : data[6],
                "longitude" : data[7],
                "room_type" : data[8],
                "price" : data[9],
                "minimum_nights" : data[10]
        }
    except Exception as e:
        return {}
    return myDict


# This is the Publisher
def send_mqtt_requests(payload):
    client = mqtt.Client()
    client.connect(broker_url, broker_port, 60)
    client.publish(topic, str(payload))
    client.disconnect()


# Reads data from the sourse CSV and calls the API for pushing the data to mosquitto
with open("../../data/data.csv") as file: 
    data = file.read()
    dataRow = data.splitlines()
    for idx, i in enumerate(dataRow):
        time.sleep(0.1)
        send_mqtt_requests(getFormattedData(dataRow))
        print("Published row " + str(idx) + ".")
