
import paho.mqtt.client as mqtt

# This is the Publisher

client = mqtt.Client()
client.connect("mqtt.eclipse.org",1883,60)
client.publish("topic/big_data_9ff5e", "Hello world!")
client.disconnect()