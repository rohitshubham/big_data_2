
#This manager dynamically loads the client scripts and executes them. The notification to load client script comes 
# as through the clientID. We have assumed that the clientID is constant in the system and hence all the client scripts
# can be traced from it. 
# The notification for ingestmanager to start the clientIngestScript is given via a MQTT signal.
# The clientID is passed as the payload on our MQTT notification. 
import logging
import paho.mqtt.client as mqtt
import subprocess
import traceback
 
custom_logging_format = '%(asctime)s : [%(levelname)s] - %(message)s'
logging.basicConfig(filename= "../../logs/mysimbdp_batchingingestmanager.log" , filemode="a", level= logging.INFO, format=custom_logging_format)

clientbatchstagingappfolder = "./clientbatchingestapp/"
noFileFoundError = "No file associated for this ID"

# This demonstration scheme is only for two clients. If there many, we can store the data into a database
def get_filename_from_clientID(client_id):
    if client_id == 1:
        return "company_1_clientbatchingestapp.py"
    elif client_id == 2:
        return "company_2_clientbatchingestapp.py" 
    else:
        return noFileFoundError



# for demonstration, we have only used python files. subprocess.Popen can open any programming language file
# in the same way, provided it's installed in the system.  
def call_relevent_client_application(client_id):
    filename = get_filename_from_clientID(int(client_id))
    if filename == noFileFoundError:
        logging.error(f"No file for this clientID {client_id} found") 
        return 0
    file_to_run = clientbatchstagingappfolder + filename
    logging.info(f"running {filename} for ClientID: {client_id}")
    try:
        subprocess.Popen("python3", 60, file_to_run)
        logging.info(f"Started clientbatchingestapp for clientID : {client_id}.")
    except Exception as e:
        logging.error(f"error while running file for clientID: {client_id}. Error is {traceback.format_exc()}")
    return 0
    



# We are using free eclipse broker for notifying that a file has been moved into staging
#the batchingingestmanager will take action when it receives data on the topic.
# Please note: this is only for notification service of batchingingestmanager and it uses a public mqtt broker.
# mysimbdp-data-broker is implemented separately. 
broker_url = "mqtt.eclipse.org"
broker_port = 1883

#topic-name = "big_data_" + truncate(sha256("big_data"), 5)
topic = "topic/big_data_9ff5e"

client = mqtt.Client()
client.connect(broker_url, broker_port)


def on_connect(client, userdata, flags, rc):
    logging.info(f"Connected to {broker_url} with result code {str(rc)}.")
    client.subscribe("topic/big_data_9ff5e")

def on_message(client, userdata, msg):
    logging.info(f"Received ingestion message for clientID: {str(msg.payload.decode())}. Proceeding to call respective client script.")
    call_relevent_client_application(str(msg.payload.decode()))
    
client = mqtt.Client()
client.connect(broker_url, broker_port)

client.on_connect = on_connect
client.on_message = on_message

client.loop_forever()