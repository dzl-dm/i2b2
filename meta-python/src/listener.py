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

## TODO: Remove - should be an admin console page, in the future
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

@app.route('/flush')
@app.route('/flush/<source_id>')
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

## TODO: This should be protected
## TODO: Config must be updated to match
@app.route('/rename')
def rename(existing_source_id:str = None, new_source_id:str = None):
    """Rename entries with matching source_id"""
    app.logger.info("Renaming database entries with source_id '{}' to: {}".format(existing_source_id, new_source_id))
    app.logger.debug("Not yet implemented")
    pass

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


@app.route('/prepare-custom-query')
@app.route('/prepare-custom-query/<source_id>')
def prepare_custom_query(source_id:str = None):
    """Use local files to prepare custom queries ready for database insertion"""
    if source_id is None:
        source_id = request.args.get('source_id')
    app.logger.info("Supplied vars...\nsource_id: {}".format(source_id))

    response = {}
    response['status_code'] = 500
    response['content'] = ""
    source_type, source_dir, source_file_paths, source_update = meta.source_info(source_id)
    if source_type == "local_files":
        source_dir = os.path.join(app.config["local_file_sources"], source_id)
        app.logger.info("Generating i2b2 data for source_id '{}' from files in directory: {}".format(source_id, source_dir))
    else:
        response["content"] = "Source type '{}' for source_id '{}' is not compatible with the update-custom-query endpoint".format(source_id)
        response['status_code'] = 500
        return response

    ## Steps:
    # Check system (directories exist etc?)
    # Get file names which need updating
    # Update csv file (creating new, database ready files)
        # add source_id's
        # add date/time's - added later in memory before db push
        # Write consistent output filename to /tmp/meta-translator
    # Push to database
    # Clear temporary files
    db_conn = connection.get_database_connection(
        os.getenv("I2B2DBHOST"),
        os.getenv("I2B2DBNAME"),
        os.getenv("DB_ADMIN_USER"),
        os.getenv("DB_ADMIN_PASS")
    )
    base_filepaths = [os.path.join(source_dir, x) for x in os.listdir(source_dir)]
    prepared_file_paths = meta.prepare_csv_files(db_conn, base_filepaths, source_id)
    ## TODO: Check here for any files which should be removed
    # - eg old files which match <db_prepared_directory>/<db_prepared_prefix>.<source_id>
    # But are not in the newly generated prepared_file_paths (because of the decoupling, we can't actually use that list!)

    ## TODO: This should only prepare - 
    ## TODO: Should this final prep-step be part of the push? Then we can use the prepared_file_paths list and it'll be slicker...
    ## Update the CSV files with timestamps and source_id - then push to database
    # prepared_file_paths = meta.prepare_custom_queries(source_id, source_dir)
    if prepared_file_paths and len(prepared_file_paths) > 0:
        response['content'] += "{}\n".format("Files prepared for source_id: {}!".format(source_id))
        if meta.push_csv_to_database(db_conn, source_id, prepared_file_paths, delim):
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
