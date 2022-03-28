""" i2b2.py
The API for i2b2. We setup flask routes to provide customisations to i2b2
"""
print("Begin i2b2.py")

## Setup logging (before importing other modules)
import logging
import logging.config
import os
import yaml
print("Basic imports done")
if not os.path.isdir("/var/log/i2b2/"):
    os.mkdir("/var/log/i2b2/")
LOG_CONF_PATH = os.getenv('LOG_CONF_PATH')
print("Setting up logging, using config file: {}".format(LOG_CONF_PATH))
if LOG_CONF_PATH is None:
    print(os.environ)
# with open("/api_base/config/logging.yaml", "r") as f:
with open(LOG_CONF_PATH, "r") as f:
    log_config = yaml.load(f, Loader=yaml.FullLoader)
logging.config.dictConfig(log_config)
## Load logger for this file
logger = logging.getLogger(__name__)
logger.debug("Logging loaded and configured")

from flask import Flask
from flask import request
from flask_accept import accept
logger.debug("Flask modules loaded")

app = Flask(__name__)
import requests

@app.route('/')
def index():
    return 'Index Page'

@app.route('/stats')
@accept('text/html')
def stats():
    logger.info("Getting stats...")
    import stats
    result = stats.get_stats()
    return "<html><body><p>Exitcode: "+str(result[1])+"</p><p>Message Log:<br/>"+result[0].replace("\n","<br/>")+"</p></body></html>"

@app.route('/updatemeta')
def updatemeta():
    """Make a basic http request to the meta container which will then handle the translation of incoming meta-data to i2b2"""
    logger.info("Updating i2b2 with CoMetaR metadata...")
    
    ## TODO: Use requesting IP and/or fuseki endpoint (to be sent as parameter?) to allow collecting data from multiple CoMetaR's
    logger.debug("Request from: {}".format(request.remote_addr))
    ## Winner below
    logger.debug("OR maybe request from: {}".format(request.environ.get('HTTP_X_REAL_IP', request.remote_addr)))
    ## Winner above
    logger.debug("OR maybe request from: {}".format(request.environ['REMOTE_ADDR']))

    meta_update_endpoint = "http://{}:5000/?fuseki_endpoint={}".format(os.getenv("META_SERVER"), os.getenv("generator_sparql_endpoint"))
    logger.debug("Forwarding request to responsible container: {}".format(meta_update_endpoint))
    meta_response = requests.get(meta_update_endpoint)
    result = [meta_response.ok, meta_response.text]
    logger.debug("Updade of meta data complete: {}".format(result))
    return "<html><body><p>Success: "+str(result[0])+"</p><p>Message Log:<br/>"+result[1].replace("\n","<br/>")+"</p></body></html>\n"
