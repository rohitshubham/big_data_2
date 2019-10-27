#!/usr/bin/python3

import pymongo
import logging
import time
import traceback
import os 

custom_logging_format = '%(asctime)s : [%(levelname)s] - %(message)s'
logging.basicConfig(filename= "../../logs/company2/batchingingestapp.log" , filemode="a", level= logging.INFO, format=custom_logging_format)


__connectionString = "mongodb://localhost:27017"
__databaseName = "mysimbdp-coredms"
__fileDirectory = "../staging/client_2_temp/"
client_id = 2
current_file_name = ""

def getFormattedData(message):
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
    return myDict

# there will be only one file to be processed at once at splitting is not enabled for this client in config.json
def get_list():
    myList = []
    global current_file_name
    total_rows_read = 0
    start = time.time()
    for file_name in os.listdir(__fileDirectory):
        current_file_name = file_name
        logging.info(f"Started reading {file_name} for uploading into the DB.")
        with open(file_name) as file: 
            data = file.read()
            dataRow = data.splitlines()
            for idx, i in enumerate(dataRow):
                myList.append(getFormattedData(i))
                total_rows_read = idx
    logging.info(f"Total rows read for file {current_file_name} are {total_rows_read}")
    end = time.time()
    logging.info(f"TIMING: the time taken to read all the rows in {current_file_name} in the DB was : {end-start}")
    return myList


def __connectToDatabase(collectionName):
    simpbdp_client = pymongo.MongoClient(__connectionString)
    getDatabase = simpbdp_client[__databaseName]
    return getDatabase[collectionName] 


def connectAndInsertRow(valueToBeStored):
    current_collection = __connectToDatabase("company_2_datasink")
    current_collection.insert_many(valueToBeStored)

def insertRowsIntoMongo():
    try:
        logging.info("Received signal for ingestion for client 2")
        myList = get_list()

        #We only need to measure the timing data for saving into the database. the reading time is being measured separately.
        start = time.time()
        connectAndInsertRow(myList)
        end= time.time()

        logging.info(f"TIMING: the time for saving the file {current_file_name} in the DB was : {end-start}")
        logging.info(f"successfully processed the file {current_file_name}. The size was : {os.path.getsize(__fileDirectory+current_file_name)}")
    except Exception as e:
        logging.error(f"error while running file for clientID: {client_id}. Error is {traceback.format_exc()}")

insertRowsIntoMongo()

