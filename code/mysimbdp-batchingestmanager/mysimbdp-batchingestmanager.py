import logging

custom_logging_format = '%(asctime)s : [%(levelname)s] - %(message)s'
logging.basicConfig(filename= "../../logs/mysimbdp_batchingingestmanager.log" , filemode="a", level= logging.INFO, format=custom_logging_format)

