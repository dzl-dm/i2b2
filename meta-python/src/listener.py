""" listener.py
The flask listener for i2b2's meta data importer
"""
print("Begin listener.py")

## Setup logging (before importing other modules)
import logging
import logging.config
import os
import yaml
print("Basic imports done")
if not os.path.isdir("/var/log/meta/"):
    os.mkdir("/var/log/meta/")
print("Setting up logging, using config file: {}".format(os.getenv('LOG_CONF_PATH')))
with open(os.getenv('LOG_CONF_PATH'), "r") as f:
    log_config = yaml.load(f, Loader=yaml.FullLoader)
logging.config.dictConfig(log_config)
## Load logger for this file
logger = logging.getLogger(__name__)
logger.debug("Logging loaded and configured")

## Import and configure the flask app
from flask import Flask
logger.debug("Flask modules loaded")
from default_config import Config as default_config
app = Flask(__name__)
app.config.from_object(default_config)
app.config.from_file(os.getenv('APP_CONF_PATH'), load=yaml.load)

## import most modules after configuring the app
# import db_connection
import meta
import model
import subprocess

## Global var(s)
is_running = False

@app.route('/')
def index():
    global is_running
    if is_running == True:
        logger.warn("Busy! Already running import. Please wait for it to complete before requesting another!")
        return "Busy! Already running import. Please wait for it to complete before requesting another!\n"
    is_running = True
    ## Use try/finally to ensure is_running is reset to false, even if there is an error
    try:
        logger.info("Started import scripts...")
        logger.info("Scripts use another logfile: {}".format("/var/log/meta.log"))
        result = meta.update_meta_from_cometar()
        # return "<html><body><p>Exitcode: "+str(result[1])+"</p><p>Message Log:<br/>"+result[0].replace("\n","<br/>")+"</p></body></html>"
        scripts = subprocess.check_output(['/import-meta.sh'],shell=True)
        ## Dummy "scripts" to return simple string
        # import time
        # time.sleep(5)
        # scripts = "Working..."
        logger.info("Import scripts complete!")
    except:
        logger.error("Something went wrong...")
        scripts = "Failed...\n{}\n".format(scripts)
    finally:
        logger.info(scripts)
        is_running = False
    return scripts

@app.route('/info', methods=['GET'])
def connection_stats():
    """
        Returns the connection stats for database connection (used for 
        debugging).
        Args: None
           
        Returns: 
            Response 200 and JSON String of connection stats - see 
            DBConnection class for more info. 
            
            Response 504 if database connection not possible
    """
    logger.info("Show database connection info")
    try:
        db = db_connection.DBConnection()
    except IOError:
        return "Database connection not possible", 504, {
            'ContentType': 'text/plain'
        }
    return db.get_connection_stats(), 200, {'ContentType': 'application/json'}

