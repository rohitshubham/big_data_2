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
argument_action = arguments[1]
argument_name = arguments[0]
argument_pid = arguments[2]

clientbatchstagingappfolder = "./clientstreamingestapp/"

#only validating client1 and client2 at the moment.
def validate_command_arguments(argument_name, argument_pid, argument_action):
    if argument_action not in ["start", "stop"]:
        logging.error(f"The argument is not correct. Please try again.")
        return 0
    if argument_name not in ["client1", "client2"]:
        logging.error(f"The argument is not correct. Please try again.")
        return 0
    if argument_action == "stop":
        try:
            int(argument_pid)
        except Exception as e:
            logging.error("The Pid is not an integer. Please provide an integer")
            return 0
    return 1

def start_script(argument_name, argument_pid, argument_action):
    file_name = "company_1_clientstreamingestapp.py" if argument_name == "client1" else "company_2_clientstreamingestapp.py"
    file_to_run = clientbatchstagingappfolder + file_name
    prc = subprocess.Popen("python3", 60, file_to_run)
    logging.info(f"ProcessID : Started the process with PID: {prc.pid}. Remember to use it to kill the process.")
    print(f"The processID created is : {prc.pid}")
    return prc.pid
    

def stop_script(argument_name, argument_pid, argument_action):
    logging.info(f"attempting to stop process for {argument_name}")
    process_id = int(argument_pid)
    subprocess.call(["kill", "-9", "%d" % process_id])
    logging.info(f"ProcessID: successfully killled the process for {argument_name}. The pID killed was {process_id} ")
    return 0

def run_command(argument_name, argument_pid, argument_action):
    try:
        if validate_command_arguments(argument_name, argument_pid, argument_action) == 1:
            logging.info("argument validation passed. Proceeding to invoke scripts.")
            if argument_action == "start":
                return start_script(argument_name, argument_pid, argument_action)
            else:
                return stop_script(argument_name, argument_pid, argument_action)
    except Exception as e:
        #verbose logging for debugging
        logging.error(f"error while running file for client. The input was {str(arguments)}. Error is {traceback.format_exc()}")

run_command(argument_name, argument_pid, argument_action)

