""" listener.py
The flask listener for i2b2's meta data importer
"""
print("Begin listener.py")

## Setup logging (before importing other modules)
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
    """Add the yaml based settings to the app context"""
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

@app.route('/')
def index(fuseki_endpoint:str = None, source_id:str = None):
    app.logger.info("Running index route defaulting to 'single' route...")
    ## For when params are supplied via ?param1=value1&param2=value2
    if fuseki_endpoint is None:
        fuseki_endpoint = request.args.get('fuseki_endpoint')
    if source_id is None:
        source_id = request.args.get('source_id')

    response = single(fuseki_endpoint, source_id)
    return response

@app.route('/fetch')
def fetch(fuseki_endpoint:str = None, source_id:str = None):
    """Fetch and serialise (as CSV files) the metadata"""
    app.logger.info("Running fetch route to fetch metadata from the fuseki_endpoint '{}'...".format(fuseki_endpoint))
    ## TODO: Allow fetching from multiple sources
        ## TODO: Protect against concurrent fetching from the same source/source_id
    pass

@app.route('/flush')
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
                response['content'] = "Source data removed from database for source_id: {}".format(source_id)
            else:
                db_conn.rollback()
                response['status_code'] = 500
                response['content'] = "Unable to clean source data in database for source_id: {}".format(source_id)
        except:
            db_conn.rollback()
            app.logger.error("Error flushing data for source_id: {}\n{}".format(source_id, Exception))
        finally:
            db_conn.close()
    else:
        response['status_code'] = 500
        response['content'] = "source_id not provided: {}".format(source_id)
    return response

@app.route('/rename')
def rename(existing_source_id:str = None, new_source_id:str = None):
    """Rename entries with matching source_id"""
    app.logger.info("Renaming database entries with source_id '{}' to: {}".format(existing_source_id, new_source_id))
    pass

@app.route('/update')
def update(source_ids:list = None):
    """Push data to i2b2 - read from data which is serialised (as JSON files) by the "fetch" route"""
    app.logger.info("Running update route to update i2b2 with pre-fetched metadata...")
    ## TODO: Check serialised data exists - else skip
        ## TODO: use source_ids list to update only the given ids, default to all
    ## TODO: Read serialised data, convert to SQL, push to i2b2 database
    ## TODO: Clean up files 
    pass

# @app.route('/single', defaults=fuseki_endpoint=None, source_id=None)
@app.route('/single')
def single(fuseki_endpoint:str = None, source_id:str = None):
    """Run end-to-end for a single source id - no intermediate output"""
    app.logger.info("Running single route to update i2b2 with metadata from a single source...")
    ## For when params are supplied via ?param1=value1&param2=value2
    if fuseki_endpoint is None:
        fuseki_endpoint = request.args.get('fuseki_endpoint')
    if source_id is None:
        source_id = request.args.get('source_id')

    app.logger.info("Supplied vars...\nfuseki_endpoint: {}\nsource_id: {}".format(fuseki_endpoint, source_id))

    response = {}
    response['status_code'] = 500
    response['content'] = ""
    if source_id in app.config["fuseki_sources"].keys():
        fuseki_endpoint = app.config["fuseki_sources"][source_id]
        app.logger.debug("Set endpoint based on recognised source ({}): {}".format(source_id, fuseki_endpoint))
    elif source_id in app.config["local_file_sources"].keys():
        app.logger.debug("Forwarding request for file-based query update: {}".format(source_id))
        return custom_query(source_id)
    else:
        app.logger.error("source_id ({}) is not recognised, will not process any further!".format(source_id))
        response['content'] += "{}\n".format("Must provide a valid source_id ({})!".format(source_id))
        response['status_code'] = 500
        return response

    ## TODO: Return error when endpoint or source_id not provided
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
    app.logger.debug("All trees for source '{}': {}".format(source_id, all_trees))
    combined_tree = meta.combine_csv_trees(all_trees)
    if meta.write_csv(combined_tree, source_id, app.config["csv_out_dir"]):
        response['content'] += "{}\n".format("CSV written")
        response['status_code'] = 200
    else:
        response['content'] += "{}\n".format("CSV writing FAILED!")
        response['status_code'] = 500
        return response

    ## Prepare CSV files for inserting and execute against database
    db_conn = connection.get_database_connection(
        os.getenv("I2B2DBHOST"),
        os.getenv("I2B2DBNAME"),
        os.getenv("DB_ADMIN_USER"),
        os.getenv("DB_ADMIN_PASS")
    )
    if meta.csv_to_database(db_conn=db_conn, out_dir=app.config["csv_out_dir"], sources=[source_id]):
        response['content'] += "{}\n".format("Database updated!")
        response['status_code'] = 200
    else:
        response['content'] += "{}\n".format("Database update FAILED!")
        response['status_code'] = 500
        return response

    app.logger.info("Processing complete!")
    app.logger.info(response)
    return response

@app.route('/custom_query/')
def custom_query(source_id:str = None):
    """Use local files to update custom queries"""
    if source_id is None:
        source_id = request.args.get('source_id')
    app.logger.info("Supplied vars...\nsource_id: {}".format(source_id))

    response = {}
    response['status_code'] = 500
    response['content'] = ""
    if source_id not in app.config["local_file_sources"].keys():
        app.logger.error("source_id ({}) is not recognised, will not process any further!".format(source_id))
        response['content'] += "{}\n".format("Must provide a valid source_id ({})!".format(source_id))
        response['status_code'] = 500
        return response

    ## Update the CSV files with timestamps and source_id - then push to database
    source_dir = app.config["local_file_sources"][source_id]
    prepared_file_paths = meta.prepare_custom_queries(source_id, source_dir)
    if prepared_file_paths and len(prepared_file_paths) > 0:
        response['content'] += "{}\n".format("Files prepared for source_id: {}!".format(source_id))
        db_conn = connection.get_database_connection(
            os.getenv("I2B2DBHOST"),
            os.getenv("I2B2DBNAME"),
            os.getenv("DB_ADMIN_USER"),
            os.getenv("DB_ADMIN_PASS")
        )
        if meta._push_prepared_csv_to_database(db_conn, prepared_file_paths):
            db_conn.commit()
            app.logger.info("Pushing data to database succeeded!")
            response['content'] += "{}\n".format("Data pushed to database!")
            response['status_code'] = 200
        else:
            app.logger.error("Pushing data to database failed!")
            response['content'] += "{}\n".format("Pushing data to database failed!")
            response['status_code'] = 500
            return response
    else:
        app.logger.error("No prepared_file_paths ({}) available - cannot push to database".format(prepared_file_paths))
        response['content'] += "{}\n".format("Processing did not produce any data for the database!".format(source_id))
        response['status_code'] = 500
        return response
    return response

@app.route('/testtop/')
def testtop():
    app.logger.info("Running testtop route...")
    from queries import queries
    from queries import connection
    fuseki_endpoint = "https://data.dzl.de/fuseki/cometar_live/query"
    connection = connection.get_fuseki_connection(fuseki_endpoint, "requests")
    top_elements:dict = queries.top_elements(connection)
    # node_uri, node_type = iter(next(top_elements))
    # queries.ge metadata_trees[source_id].append(get_tree(sparql_wrapper, node_uri, node_type))
    connection["session"].close()
    return {"status": 200, "content": "Test complete - see logs\n{}".format(top_elements)}

@app.route('/testtree')
def testtree():
    app.logger.info("Running testtree route...")
    import meta
    result = None
    response = {}
    app.logger.info("Running test functions...")
    response['status_code'] = 200
    response['content'] = ""
    node_url = "http://loinc.org/owl#2458-8"
    fuseki_endpoint = "https://data.dzl.de/fuseki/cometar_live/query"
    source_id = "test"

    from queries import connection
    connection = connection.get_fuseki_connection(fuseki_endpoint, "requests")

    result = meta.get_tree(connection, node_url, "concept")
    sql_trees = result.whole_tree_inserts()
    app.logger.info("## ** ---------- ---------- ---------- ** ##")
    app.logger.info("Whole tree SQL inserts:\n{}".format(sql_trees))
    app.logger.info("## ** ---------- ---------- ---------- ** ##")
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

