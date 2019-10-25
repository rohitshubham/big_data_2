import logging
import paho.mqtt.client as mqtt


custom_logging_format = '%(asctime)s : [%(levelname)s] - %(message)s'
logging.basicConfig(filename= "../../logs/mysimbdp_batchingingestmanager.log" , filemode="a", level= logging.INFO, format=custom_logging_format)

#We are using free hiveMQ broker for notifying that a file has been moved into staging
#the batchingingestmanager will take action when it receives data on the topic.
# Please note: this is only for notification service of batchingingestmanager and it uses a public mqtt broker.
# mysimbdp-data-broker is implemented separately. 
broker_url = "mqtt.eclipse.org"
broker_port = 1883

client = mqtt.Client()
client.connect(broker_url, broker_port)

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("topic/big_data_9ff5e")

def on_message(client, userdata, msg):
    print(msg.payload.decode())
    
client = mqtt.Client()
client.connect(broker_url, broker_port)

client.on_connect = on_connect
client.on_message = on_message

client.loop_forever()