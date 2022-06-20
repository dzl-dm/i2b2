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

@app.route('/updatemeta/<source_id>')
@app.route('/updatemeta')
def updatemeta(source_id:str = None):
    """Make a simple http request to the meta container which will then handle the translation of incoming meta-data to i2b2"""
    if source_id is None:
        source_id = request.args.get('source_id')
    logger.info("Updating i2b2 metadata from source: '{}'...".format(source_id))
    if source_id is None:
        message = "Client must supply a source_id - use \"all\" to update all known sources"
        logger.error(message)
        return "<html><body><p>Success: False</p><p>Message Log:<br/>Code: 400<br/>Message: {}</p></body></html>\n".format(message)

    ## TODO: Use requesting IP and/or fuseki endpoint (to be sent as parameter?) to allow collecting data from multiple CoMetaR's
    logger.debug("Request from: {}".format(request.remote_addr))
    ## Winner below
    logger.debug("OR maybe request from: {}".format(request.environ.get('HTTP_X_REAL_IP', request.remote_addr)))
    ## Winner above
    logger.debug("OR maybe request from: {}".format(request.environ['REMOTE_ADDR']))

    # meta_update_endpoint = "http://{meta_server}:5000/single?source_id={source_id}".format(
    #     meta_server = os.getenv("META_SERVER"),
    #     source_id = os.getenv("generator_sparql_endpoint")
    #     )
    # logger.debug("Forwarding request to responsible container: {}".format(meta_update_endpoint))
    # meta_response = requests.get(meta_update_endpoint)
    meta_fetch = "http://{meta_server}:5000/fetch-and-generate-csv?source_id={source_id}".format(
        meta_server = os.getenv("META_SERVER"),
        source_id = source_id
        )
    meta_flush = "http://{meta_server}:5000/flush-metadata?source_id={source_id}".format(
        meta_server = os.getenv("META_SERVER"),
        source_id = source_id
        )
    meta_load = "http://{meta_server}:5000/load-csv-to-postgres?source_id={source_id}".format(
        meta_server = os.getenv("META_SERVER"),
        source_id = source_id
        )
    meta_count_patients = "http://{meta_server}:5000/update-patient-counts".format(
        meta_server = os.getenv("META_SERVER")
        )

    result = [False, "No results"]
    meta_response = requests.get(meta_fetch)
    ## For subsequent "result"s, do not override "False" if the latest response is true. Append text instead of overwrite 
    result = [meta_response.ok, meta_response.text]
    if meta_response:
        meta_response = requests.get(meta_flush)
        result = [not result[0] or meta_response.ok, result[1] + meta_response.text]
        meta_response = requests.get(meta_load)
        result = [not result[0] or meta_response.ok, result[1] + meta_response.text]
    meta_response = requests.get(meta_count_patients)
    result = [not result[0] or meta_response.ok, result[1] + meta_response.text]

    # result = [meta_response.ok, meta_response.text]
    logger.debug("Updade of meta data complete: {}".format(result))
    ## TODO: Return a (jinja2?) template to display all result[1]'s - should be a list of dict's with status and content
    return "<html><body><p>Success: {endpoint_status}</p><p>Message Log:<br/>{endpoint_messages}</p></body></html>\n".format(
        endpoint_status = str(result[0]),
        endpoint_messages = result[1].replace("\n","<br/>")
    )

@app.route('/flushmeta/<source_id>')
@app.route('/flushmeta')
def flushmeta(source_id:str = None):
    """Make a basic http request to the meta container which will then handle the removal of appropriate meta-data in i2b2"""
    if source_id is None:
        ## Allow ?source_id=xyz in addition to /<source_id>
        source_id = request.args.get('source_id')
    logger.info("Flushing i2b2 metadata with source_id: '{}'...".format(source_id))

    meta_update_endpoint = "http://{meta_server}:5000/flush-metadata?source_id={source_id}".format(
        meta_server = os.getenv("META_SERVER"),
        source_id = source_id
        )
    logger.debug("Forwarding request to responsible container: {}".format(meta_update_endpoint))
    meta_response = requests.get(meta_update_endpoint)
    result = [meta_response.ok, meta_response.text]
    logger.debug("Flushing/clearing of meta with source_id '{}' in i2b2 data complete: {}".format(source_id, result))
    return "<html><body><p>Success: {endpoint_status}</p><p>Message Log:<br/>{endpoint_messages}</p></body></html>\n".format(
        endpoint_status = str(result[0]),
        endpoint_messages = result[1].replace("\n","<br/>")
    )

@app.route('/update-patient-counts')
def update_patient_counts():
    """Make a basic http request to the meta container which will then handle the updating of patient counts in i2b2"""
    logger.info("Updating i2b2 patient counts...")

    meta_update_endpoint = "http://{meta_server}:5000/update-patient-counts".format(
        meta_server = os.getenv("META_SERVER")
        )
    logger.debug("Forwarding request to responsible container: {}".format(meta_update_endpoint))
    meta_response = requests.get(meta_update_endpoint)
    result = [meta_response.ok, meta_response.text]
    logger.debug("Patient counts updated: {}".format(result))
    return "<html><body><p>Success: {endpoint_status}</p><p>Message Log:<br/>{endpoint_messages}</p></body></html>\n".format(
        endpoint_status = str(result[0]),
        endpoint_messages = result[1].replace("\n","<br/>")
    )
