from os import listdir
from os.path import isfile, join, getsize
import os
import json
import logging
import re
import shutil
import time 

custom_logging_format = '%(asctime)s : [%(levelname)s] - %(message)s'
logging.basicConfig(filename= "../../logs/mysimbdp_fetchData.log" , filemode="a", level= logging.INFO, format=custom_logging_format)


isProcessingFile = False
path = './client-input-directory/'
config_path = "config.json"
temp_client_folder = ""
client_name = ""
log = ""

def does_file_exist_in_dir(path):
    return any(isfile(join(path, i)) for i in listdir(path))

def get_config_data():
    with open(config_path) as f:
        return json.load(f)

def do_validation_on_Files(filePath):
    error_list = []
    config_data = get_config_data()

    #size of file
    if config_data["Max_file_size"] < getsize(filePath):
        error_list.append("The size of document is larger!")
    #size of filename
    if config_data["Max_filename_length"] < len(filePath) - len(path):
        error_list.append("The name of the file is larger than 20 characters")
    return error_list


def matchFilePatternExtension(company_config, fileName):
    acceptedFileFormats = company_config["Accepted_formats"].split(",")
    for fileFormat in acceptedFileFormats:
        if fileName.endswith(fileFormat):
            return 0
    return 1

# Thanks to https://www.oreilly.com/library/view/programming-python-second/0596000855/ch04s02.html
def split_file(fromfile, todir, chunksize, client_name): 
    if not os.path.exists(todir):                  
        os.mkdir(todir)                            
    else:
        for fname in os.listdir(todir):           
            os.remove(os.path.join(todir, fname)) 
    partnum = 0
    input = open(fromfile, 'rb')                  
    while 1:                                       
        chunk = input.read(chunksize)         
        if not chunk: break
        partnum  = partnum+1
        filename = os.path.join(todir, (client_name + '-part%04d' % partnum))
        fileobj  = open(filename, 'wb')
        fileobj.write(chunk)
        fileobj.close()                            
    input.close()
    assert partnum <= 9999
    logging.info(f"The file has been split into {partnum + 1 } files.")                         
    return partnum

# Status Code: 0 = Successful , 1 = Unsuccessful
def client_validations(fileName, path):
    company_1_config = get_config_data()["Company_1_Profile"]
    company_2_config = get_config_data()["Company_2_Profile"]
    global temp_client_folder, client_name

    if re.match(company_1_config["File_Regex"], fileName):
        logging.info(f"File name {fileName} matches with Company 1's profile.")
        logging.info("starting company 1 checks before moving the file")
        
        if matchFilePatternExtension(company_1_config, fileName) == 1:
            logging.warning(f"{fileName} cannot be processed as it does not match any of the accepted formats.")
            return 1

        temp_client_folder = "client_1_temp/"
        client_name = "comp_1"
        if company_1_config["Allow_Split"] == "True" and int(company_1_config["Block_size"]) < getsize(path + fileName):
            # it means we need to split the file before transferring (micro-batching)            
            split_file(path + fileName, path + temp_client_folder, company_1_config["Block_size"], client_name)
        else:
            if not os.path.exists(path + temp_client_folder):                  
                os.mkdir(path + temp_client_folder)  
            shutil.move(path+fileName, path+temp_client_folder+fileName)
        return 0
    elif re.match(company_2_config["File_Regex"], fileName):
        logging.info(f"File name {fileName} matches with Company 2's profile.")
        logging.info("starting company 2 checks before moving the file")
        
        if matchFilePatternExtension(company_2_config, fileName) == 1:
            logging.warning(f"{fileName} cannot be processed as it does not match any of the accepted formats.") 
            return 1
        temp_client_folder = "client_2_temp/"
        client_name = "comp_2"
        if company_2_config["Allow_Split"] == "True" and int(company_2_config["Block_size"]) < getsize(path + fileName):
            # it means we need to split the file before transferring (micro-batching)
            logging.info("Splitting the files in small batches.")
            split_file(path + fileName, path + temp_client_folder, company_2_config["Block_size"], client_name)
        else:
            if not os.path.exists(path + temp_client_folder):                  
                os.mkdir(path + temp_client_folder)      
            shutil.move(path+fileName, path+temp_client_folder+fileName)
     
        return 0
    else:
        logging.warning("The file did not match any regex of any client's profile. Hence cannot be processed.")
        return 1

# we use shutil library for movement. This is able even move files across disks and network 
def move_file_to_staging(staging_location):
    for fileName in listdir(path+temp_client_folder):
        if not os.path.exists(staging_location+temp_client_folder):                  
            os.mkdir(staging_location+temp_client_folder)
        shutil.move(path+temp_client_folder+fileName, staging_location+temp_client_folder+fileName)
        logging.info(f"successfully moved file {fileName}")
    return 0

def move_file_to_discard(filePath, discarded_location, fileName):
    shutil.move(filePath, discarded_location+fileName)
    return 0

def checkIfFIleExists():
    if does_file_exist_in_dir(path):
        do_validation_on_Files
        files = listdir(path)

        for file_name in files:
            start = time.time()
            # We are doing a custom log adapter for python. This ensures the file name are always printed automatically
            filePath = path + file_name
            # do validations
            error_list = do_validation_on_Files(filePath)
            if len(error_list) > 0:
                print(error_list)
                logging.warning(f"Ignoring {file_name}! It was not accepted due to following errors: {', '.join(error_list)}.")
                move_file_to_discard(filePath, "./discarded", file_name)
                end = time.time()
                logging.info(f"TIMING: Time taken to process {file_name} is {end-start}")
            else:
                logging.info(f"File {file_name} has passed the generic validation checks")
                logging.info("Now starting client specific checks.")
                result_code = client_validations(file_name, path)
                if result_code == 1:
                    logging.warning("Moving the file to discarded directory!")
                    move_file_to_discard(filePath, "./discarded", file_name)
                    end = time.time()
                    logging.info(f"TIMING: Time taken to process {file_name} is {end-start}")
                else:
                    logging.info("all validations completed successfully. Now moving the file to staging environments.")
                    move_file_to_staging("./staging/")
                    shutil.rmtree(path+temp_client_folder)
                    end = time.time()
                    logging.info(f"TIMING: Time taken to process {file_name} is {end-start}")
 

#checks the folder for new file every second
while True:
    checkIfFIleExists()
    time.sleep(1)




    
        



