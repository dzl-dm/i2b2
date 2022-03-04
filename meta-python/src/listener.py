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
# logger = logging.getLogger(__name__)
# logger.debug("Logging loaded and configured")

def load_settings(app_context):
    """Add the yaml based settings to the app context"""
    app_settings:dict = {}
    with open(os.getenv("APP_CONF_PATH"), "r") as yaml_file:
        app_settings = yaml.safe_load(yaml_file)
    for k, v in app_settings.items():
        app_context.config[k] = v

## Import and configure the flask app
from flask import Flask
# logger.debug("Flask modules loaded, attempting to configure app with: {}".format(os.getenv('APP_CONF_PATH')))
from default_config import Config as default_config
app = Flask(__name__)
app.config.from_object(default_config)

## Load user file settings
# app.config.from_file("{}".format(os.getenv('APP_CONF_PATH')), load="yaml.load")
load_settings(app)
app.logger.debug("Flask app loaded and configured with: {}".format(os.getenv('APP_CONF_PATH')))

# app.logger.debug("SQLALCHEMY_DATABASE_URI: {}".format(app.config['SQLALCHEMY_DATABASE_URI']))
# app.logger.debug("FLASK_APP: {}".format(app.config['FLASK_APP']))
# app.logger.debug("FLASK_ENV: {}".format(app.config['FLASK_ENV']))

## import most modules after configuring the app. NO - Need to be imported in an app route context for the context to be setup and inheritable (other ways may be possible? - the factory method?)
# import db_connection
# from model import db
# import meta
# import subprocess

## Global var(s)
is_running = False

@app.route('/')
def index():
    app.logger.info("Running index route...")
    import meta
    import subprocess
    global is_running
    if is_running == True:
        app.logger.warn("Busy! Already running import. Please wait for it to complete before requesting another!")
        return "Busy! Already running import. Please wait for it to complete before requesting another!\n"
    is_running = True
    ## Use try/finally to ensure is_running is reset to false, even if there is an error
    scripts = None
    try:
        app.logger.info("Started import scripts...")
        app.logger.info("Scripts use another logfile: {}".format("/var/log/meta.log"))
        result = meta.update_meta_from_cometar()
        # return "<html><body><p>Exitcode: "+str(result[1])+"</p><p>Message Log:<br/>"+result[0].replace("\n","<br/>")+"</p></body></html>"
        scripts = subprocess.check_output(['/import-meta.sh'],shell=True)
        ## Dummy "scripts" to return simple string
        # import time
        # time.sleep(5)
        # scripts = "Working..."
        app.logger.info("Import scripts complete!")
    except:
        app.logger.error("Something went wrong...")
        scripts = "Failed...\n{}\n".format(scripts)
    finally:
        app.logger.info(scripts)
        is_running = False
    return scripts

@app.route('/test')
def test():
    app.logger.info("Running test route...")
    import meta
    scripts = None
    result = None
    response = {}
    app.logger.info("Started import scripts...")
    app.logger.info("Shell scripts use another logfile: {}".format("/var/log/meta.log"))
    # result = meta.update_meta_from_cometar()
    # return "<html><body><p>Exitcode: "+str(result[1])+"</p><p>Message Log:<br/>"+result[0].replace("\n","<br/>")+"</p></body></html>"
    # scripts = subprocess.check_output(['/import-meta.sh'],shell=True)
    response['status_code'] = 200
    response['content'] = ""
    # result = meta.pull_fuseki_data2()
    # response['content'] += "{}\n".format(result)
    result = meta.pull_fuseki_data3()
    response['content'] += "{}\n".format(result)
    ## Dummy "scripts" to return simple string
    # import time
    # time.sleep(5)
    # scripts = "Working..."
    app.logger.info("Import scripts complete!")
    app.logger.info(response)
    return response

@app.route('/test2')
def test2():
    app.logger.info("Running test2 route...")
    import meta
    result = None
    response = {}
    app.logger.info("Running test functions...")
    response['status_code'] = 200
    response['content'] = ""
    result = meta.get_element()
    response['content'] += "{}\n".format(result)
    ## Dummy "scripts" to return simple string
    # import time
    # time.sleep(5)
    # scripts = "Working..."
    app.logger.info("Test complete!")
    app.logger.info(response)
    return response

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
    from model import db
    app.logger.info("Show database connection info")
    try:
        db_test = db.DBConnection()
    except IOError:
        return "Database connection not possible", 504, {
            'ContentType': 'text/plain'
        }
    return db_test.get_connection_stats(), 200, {'ContentType': 'application/json'}

