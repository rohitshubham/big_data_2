import pymongo
import gridfs
from datetime import datetime
import os 
import logging
import time

custom_logging_format = '%(asctime)s : [%(levelname)s] - %(message)s'
logging.basicConfig(filename= "../../logs/company1/batchingingestapp.log" , filemode="a", level= logging.INFO, format=custom_logging_format)


# mongodb connection strings
#Strictly internal APIs to call and save data in MongoDB
# it requires "gridfs" to save the file into db

__connectionString = "mongodb://localhost:27017"
__databaseName = "mysimbdp-coredms"
__fileDirectory = "../../staging/client_1_temp/"

#Since this a client's app, the collectionName is always same 
collectionName = "company_1_filesink" 

def __connectToDatabase():
    simpbdp_client = pymongo.MongoClient(__connectionString)
    getDatabase = simpbdp_client[__databaseName]
    return getDatabase 
     
# remember that this client need to join the file
def join_files(fromdir, tofile):    
    output = open(tofile, 'wb')
    parts  = os.listdir(fromdir)
    parts.sort(  )
    for filename in parts:
        filepath = os.path.join(fromdir, filename)
        fileobj  = open(filepath, 'rb')
        while 1:
            filebytes = fileobj.read(1024)
            if not filebytes: break
            output.write(filebytes)
        fileobj.close()
    output.close()


def __connectAndInsertRow(valueToBeStored):
    current_collection = __connectToDatabase()[collectionName]
    current_collection.insert_one(valueToBeStored)


def __insertFileIntoGridFs():
    fs = gridfs.GridFS(__connectToDatabase())
    
    targetFileName = "Company_1_test_split_file"
    #if there are more than one file, then we need to join them
    if len([name for name in os.listdir(__fileDirectory) if os.path.isfile(os.path.join(__fileDirectory, name))]) > 1:
        start = time.time()
        join_files(__fileDirectory, targetFileName)
        end=time.time()
        logging.info(f"TIMING: the time for joining the file was : {start-end}")

    for file_name in os.listdir(__fileDirectory):
        start = time.time()
        file = open(file_name)        
        id = fs.put(file)
        file.close()
        data = {
            "fileId" : id,
            "filename" : file_name,
            "extension" : file_name[-4:],
            "saveTime" : datetime.utcnow()
        }
        __connectAndInsertRow(data)
        end = time.time()
        logging.info(f"TIMING: the time for saving the file {file_name} in the DB was : {start-end}")
        logging.info(f"successfully processed the file {file_name}. The file ID was {id}. The size was : {os.path.getsize(__fileDirectory+file_name)}")

__insertFileIntoGridFs()