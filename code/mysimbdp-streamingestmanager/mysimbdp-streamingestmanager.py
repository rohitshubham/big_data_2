#!/usr/bin/python3

# the shell script is responsible for client caling the clientingestionapp 
# the arguments are passed into the shell script
# Argument 1 : Name of Client
# Argument 2 : Action to perform
# Argument 3 : Pid to stop (in case of stop) 
import sys
import logging 
import traceback
import subprocess

custom_logging_format = '%(asctime)s : [%(levelname)s] - %(message)s'
logging.basicConfig(filename= "../../logs/mysimbdp_streamingingestmanager.log" , filemode="a", level= logging.INFO, format=custom_logging_format)

arguments = sys.argv[1:]
clientbatchstagingappfolder = "./clientstreamingestapp/"

#only validating client1 and client2 at the moment.
def validate_command_arguments():
    if arguments[1] not in ["start", "stop"]:
        logging.error(f"The argument is not correct. Please try again.")
        return 0
    if arguments[0] not in ["client1", "client2"]:
        logging.error(f"The argument is not correct. Please try again.")
        return 0
    if arguments[1] == "stop":
        try:
            int(arguments[2])
        except Exception as e:
            logging.error("The Pid is not an integer. Please provide an integer")
            return 0
    return 1

def start_script():
    file_name = "company_1_clientstreamingestapp.py" if arguments[0] == "client1" else "company_2_clientstreamingestapp.py"
    file_to_run = clientbatchstagingappfolder + file_name
    prc = subprocess.Popen("python3", 60, file_to_run)
    logging.info(f"ProcessID : Started the process with PID: {prc.pid}. Remember to use it to kill the process.")
    print(f"The processID created is : {prc.pid}")
    

def stop_script():
    logging.info(f"attempting to stop process for {arguments[0]}")
    process_id = int(arguments[2])
    subprocess.call(["kill", "-9", "%d" % process_id])
    logging.info(f"ProcessID: successfully killled the process for {arguments[0]}. The pID killed was {process_id} ")


def run_command():
    try:
        if validate_command_arguments() == 1:
            logging.info("argument validation passed. Proceeding to invoke scripts.")
            if arguments[1] == "start":
                start_script()
            else:
                stop_script()
    except Exception as e:
        #verbose logging for debugging
        logging.error(f"error while running file for client. The input was {str(arguments)}. Error is {traceback.format_exc()}")

run_command()

