""" listener.py
The flask listener for i2b2's meta data importer
"""
print("Begin listener.py")

## Setup logging (before importing other modules)
from asyncio.log import logger
import logging
import logging.config
import os
from queries import connection
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
    """Add the yaml based settings to the app context
    
    First load user settings, then internal settings (so they override the user in case they use the same variable name)
    """
    app_settings:dict = {}
    with open(os.getenv("USER_CONF_PATH"), "r") as yaml_file:
        app_settings = yaml.safe_load(yaml_file)
    for k, v in app_settings.items():
        app_context.config[k] = v
    app_settings:dict = {}
    with open(os.getenv("APP_CONF_PATH"), "r") as yaml_file:
        app_settings = yaml.safe_load(yaml_file)
    for k, v in app_settings.items():
        app_context.config[k] = v

## Import and configure the flask app
from flask import Flask
from flask import request
from default_config import Config as default_config

app = Flask(__name__)
app.config.from_object(default_config)

## Load user file settings
load_settings(app)
app.logger.debug("Flask app loaded and configured with: {}".format(os.getenv('APP_CONF_PATH')))
## TODO: Load .env based settings (which are needed when we don't want to rebuild the docker container!)
## TODO: Or maybe better to mount the yaml config?

import meta

## Global var(s)
## TODO: Track state for each source_id?
is_running = False

## TODO: Should be an admin console page, in the future
@app.route('/')
def index():
    app.logger.info("Running index route...")

    response = "<html><body><h1>I2B2 API</h1><p>This page does not yet have any content. Please try an alternative endpoint to utilise the functionality which has been implemented</p></body></html>\n"
    return response

@app.route('/flush-metadata')
@app.route('/flush-metadata/<source_id>')
def flush(source_id:str = None):
    """Flush any existing data with the given source id - both local files and i2b2 database?"""
    app.logger.info("Running flush route to remove metadata with source_id '{}' from i2b2 and local files...".format(source_id))
    ## TODO: Should we allow flushing local files and database separately?
    response = {}
    response['status_code'] = 500
    response['content'] = ""
    if source_id is None:
        source_id = request.args.get('source_id')
    if source_id:
        app.logger.info("Attempting to remove data associated with source: {}".format(source_id))
        try:
            db_conn = connection.get_database_connection(
                os.getenv("I2B2DBHOST"),
                os.getenv("I2B2DBNAME"),
                os.getenv("DB_ADMIN_USER"),
                os.getenv("DB_ADMIN_PASS")
            )
            if meta.clean_sources_in_database(db_conn, [source_id]):
                db_conn.commit()
                response['status_code'] = 200
                response['content'] = "Source data removed from database for source_id: '{}'".format(source_id)
            else:
                db_conn.rollback()
                response['status_code'] = 500
                response['content'] = "Unable to clean source data in database for source_id: '{}'".format(source_id)
        except:
            db_conn.rollback()
            app.logger.error("Error flushing data for source_id: '{}'\n{}".format(source_id, Exception))
        finally:
            db_conn.close()
    else:
        response['status_code'] = 400
        response['content'] = "source_id not provided: '{}'".format(source_id)
    return response

@app.route('/fetch-and-generate-csv')
@app.route('/fetch-and-generate-csv/<source_id>')
def fetch(fuseki_endpoint:str = None, source_id:str = None):
    """Fetch and serialise (as CSV files) the metadata
    
    NOTE: This does not fully prepare the CSV files for the database - source_id, current_timestamps and other checks are still required
    """
    app.logger.info("Running fetch route to fetch metadata from the fuseki_endpoint '{}' and write to CSV files...".format(fuseki_endpoint))
    ## TODO: Allow fetching from multiple sources
        ## TODO: Protect against concurrent fetching from the same source/source_id
    ## For when params are supplied via query string eg ?param1=value1&param2=value2
    if fuseki_endpoint is None:
        fuseki_endpoint = request.args.get('fuseki_endpoint')
    if source_id is None:
        source_id = request.args.get('source_id')
    ## TODO: if source_id is STILL None, try to use the requesting IP to match a source (So that CoMetaR instances don't have to know their source_id)

    app.logger.info("Supplied vars...\nfuseki_endpoint: {}\nsource_id: {}".format(fuseki_endpoint, source_id))

    ## Steps:
    # Check filesystem (directories exist etc?)
    # Build data structure in memory
    # Output as csv with minimal data

    response = {}
    response['status_code'] = 500
    response['content'] = ""
    source_type, source_dir, source_file_paths, source_update = meta.source_info(source_id)
    if source_type == "fuseki":
        fuseki_endpoint = app.config["fuseki_sources"][source_id]
        app.logger.debug("Set endpoint based on recognised source ({}): {}".format(source_id, fuseki_endpoint))
    elif source_type == "local_files":
        response["content"] = "source_id '{}' is a local file based source, so there is nothing to fetch, CSV already exists".format(source_id)
        response['status_code'] = 200
        app.logger.warn(response["content"])
        return response
    else:
        response["content"] = "Unknown source_id '{}'. Please ensure its configured or defined correctly".format(source_id)
        response['status_code'] = 400
        app.logger.warn(response["content"])
        return response

    ## TODO: Return error when endpoint not available
    # if fuseki_endpoint is None or source_id is None:
    #     ## Data passed to this container is from the trusted internal docker application network, so we don't check it again - the api checks the endpoint and source are valid 
    #     app.logger.error("Route must be provided with a fuseki_endpoint and source_id! Using defaults...")
    #     fuseki_endpoint = "http://dwh.proxy/fuseki/cometar_live/query"
    #     source_id = "test"

    result = meta.pull_fuseki_datatree(fuseki_endpoint, source_id)
    if result:
        response['content'] += "{} - {}\n".format("Data retrieved from fuseki", fuseki_endpoint)
        response['status_code'] = 200
    else:
        response['content'] += "{}\n".format("Error in retrieving or processing fuseki data!")
        response['status_code'] = 500
        return response

    ## Write objects to flat structured CSV files - 1 per table
        ## Filename includes source_id
    all_trees = []
    for tree in result[source_id]:
        all_trees.append(tree.whole_tree_csv())
    app.logger.debug("All trees for source '{}': {}".format(source_id, len(all_trees)))
    combined_tree = meta.combine_csv_trees(all_trees)
    if meta.write_csv(combined_tree, source_id, source_dir):
        response['content'] += "{}\n".format("CSV written")
        response['status_code'] = 200
    else:
        response['content'] += "{}\n".format("CSV writing FAILED!")
        response['status_code'] = 500
    logger.debug("Fetching complete, response: {}".format(response))
    return response

@app.route('/load-csv-to-postgres')
@app.route('/load-csv-to-postgres/<source_id>')
def load_data(source_id:list = None):
    """Push local file based data to i2b2
    
    These include data which are serialised by the "fetch" route and locally maintained files for custom metadata
    Temporary files are named on the convention <db_prepared_directory>/<db_prepared_prefix>.<source_id>.<schema_name>.<table_name>.csv
    """
    app.logger.info("Running update route to update i2b2 with pre-fetched metadata...")
    ## TODO: Check serialised data exists - else skip
        ## TODO: use source_id list to update only the given ids, default to none, allow "all"?
    ## TODO: Read serialised data, convert to SQL, push to i2b2 database
    ## TODO: Clean up files? Let them stay, they'll be updated at the next fetch
    ## For when params are supplied via query string eg ?param1=value1&param2=value2
    if source_id is None:
        source_id = request.args.get('source_id')

    app.logger.info("Supplied vars... source_id: {}".format(source_id))

    ## Steps:
    # Check filesystem (directories exist etc?)
    # Update csv file (creating new, database ready files)
        # Add source_id
        # Add date/time
    # Push to database
    # Clear temporary files

    response = {}
    response['status_code'] = 500
    response['content'] = ""
    source_type, source_dir, source_file_paths, source_update = meta.source_info(source_id)
    ## TODO: Adjust these next lines for this function
    if source_type == "fuseki":
        fuseki_endpoint = app.config["fuseki_sources"][source_id]
        response['content'] = "Set endpoint based on recognised source ({}): {}".format(source_id, fuseki_endpoint)
        # app.logger.info(response['content'])
    elif source_type == "local_files":
        response["content"] = "source_id '{}' is a local file based source.".format(source_id)
        # app.logger.info(response["content"])
    else:
        response["content"] = "Unknown source_id '{}' (type: {}). Please ensure its configured or defined correctly".format(source_id, source_type)
        app.logger.warn(response["content"])
        return response
    ## TODO: source_dir must be under <csv_out_dir> for manual or remote sources - clarify structure
    ## Then: create the temp files with source_id's injected - necessary? Everything else is done in memory, that might be better too?
    ## Then: add dates and check col lenths etc before preparing data strings for upload/insert
    
    # source_dir = app.config["db_prepared_directory"]
    # base_filepaths = [os.path.join(source_dir, x) for x in os.listdir(source_dir)]
    # prepared_file_paths = [x for x in base_filepaths if x.startswith(app.config["db_prepared_prefix"]) and x.split(".")[1] == source_id]
    # if not prepared_file_paths or len(prepared_file_paths) == 0:
    if not source_file_paths or len(source_file_paths) == 0:
        new_message = "No prepared files matching source_id: {} ({})".format(source_id, source_file_paths)
        response["content"] += "\n{}".format(new_message)
        app.logger.warn(new_message)
        return response
    db_conn = connection.get_database_connection(
        os.getenv("I2B2DBHOST"),
        os.getenv("I2B2DBNAME"),
        os.getenv("DB_ADMIN_USER"),
        os.getenv("DB_ADMIN_PASS")
    )
    if source_type == "fuseki":
        delim = ","
    else:
        delim = ";"
    # if meta.push_csv_to_database(db_conn, prepared_file_paths):
    if meta.push_csv_to_database(db_conn, source_id, source_file_paths, delim):
        db_conn.commit()
        new_message = "Pushing CSV metadata to database has succeeded!"
        response['content'] += "\n{}".format(new_message)
        app.logger.info(new_message)
        response['status_code'] = 200
    else:
        app.logger.error("Pushing data to database failed!")
        response['content'] += "{}\n".format("Pushing CSV metadata to database failed!")
        response['status_code'] = 500
        return response

    app.logger.info("API endpoint processing complete!")
    app.logger.debug(response)
    return response

@app.route('/update-patient-counts')
def update_patient_counts():
    """Update the patient counts (in parenthesis in the tree)"""
    app.logger.info("Running route to update patient counts...")

    response = {}
    response['status_code'] = 500
    response['content'] = ""
    db_conn = connection.get_database_connection(
        os.getenv("I2B2DBHOST"),
        os.getenv("I2B2DBNAME"),
        os.getenv("DB_ADMIN_USER"),
        os.getenv("DB_ADMIN_PASS")
    )
    if meta.update_patient_count(db_conn=db_conn):
        response['content'] += "{}\n".format("Database updated with patient counts!")
        response['status_code'] = 200
        app.logger.info("Processing successfully completed!")
    else:
        response['content'] += "{}\n".format("Database update of patient counts FAILED!")
        response['status_code'] = 500
        app.logger.warn("Processing failed!")

    app.logger.info(response)
    return response
