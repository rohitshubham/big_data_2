
import time 
import paho.mqtt.client as mqtt

#topic-name = "big_data_" + truncate(sha256("company_1"), 5)
topic = "topic/big_data_850cb"
broker_url = "localhost"
broker_port = 1883

# this will send different columns as comapared to company 1
# the data model is enforced in the same manner as that of company 1, the values are different 
# if we notice both are companies are sending airbnb dictionary one at a time. however, the columns are 
# different.
def getFormattedData(message):
    try:
        data = message.split(",")
        myDict = {  "id" : data[0],
                "name" : data[1],
                "host_name" : data[3],
                "latitude" : data[6],
                "longitude" : data[7],
                "room_type" : data[8],
                "price" : data[9],
                "minimum_nights" : data[10],
                "number_of_reviews": data[11],
                "last_review" : data[12],
                "reviews_per_month" : data[13]
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
