
import paho.mqtt.client as mqtt

# This is the Publisher
def send_mqtt_requests(client_id):
    client = mqtt.Client()
    client.connect("mqtt.eclipse.org",1883,60)
    client.publish("topic/big_data_9ff5e", str(client_id))
    client.disconnect()

