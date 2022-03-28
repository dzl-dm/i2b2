""" listener.py
The flask listener for i2b2's meta data importer
"""
print("Begin listener.py")

## Setup logging (before importing other modules)
import logging
import logging.config
import os
from queries import connection
from threading import local
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
    """Fetch and serialise (as JSON files) the metadata"""
    app.logger.info("Running fetch route to fetch metadata from the fuseki_endpoint '{}'...".format(fuseki_endpoint))
    ## TODO: Allow fetching from multiple sources
        ## TODO: Protect against concurrent fetching from the same source/source_id
    pass

@app.route('/flush')
def flush(source_id:str = None):
    """Flush any existing data with the given source id - both local files and i2b2 database"""
    app.logger.info("Running flush route to remove metadata with source_id '{}' from i2b2 and local files...".format(source_id))
    ## TODO: Should we allow flushing local files and database separately?
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
    # static_locals = locals().copy()
    # app.logger.debug("static 'locals': {}".format(static_locals))
    # app.logger.debug("query_string: {}".format(request.query_string))
    # ## For when params are supplied via ?param1=value1&param2=value2
    # for param, val in static_locals.items():
    #     app.logger.debug("Param '{}': {}".format(param, val))
    #     if val is None:
    #         val = request.args.get(param)
    #         app.logger.debug("Updating param '{}' to: {}".format(param, val))
    #         # app.logger.debug("Updating param '{}' to: {}".format(param, request.args.get(param)))
    #         app.logger.debug("Running: {}".format(f'{param}="{val}"'))
    #         exec(f'{param}="{val}"')
    # app.logger.debug("plain 'locals'2: {}".format(locals()))
    # app.logger.debug("fuseki_endpoint: {}".format(fuseki_endpoint))

    app.logger.info("Running single route to update i2b2 with metadata from a single source...")
    ## For when params are supplied via ?param1=value1&param2=value2
    if fuseki_endpoint is None:
        fuseki_endpoint = request.args.get('fuseki_endpoint')
    if source_id is None:
        source_id = request.args.get('source_id')
    import meta

    ## For when params are supplied via ?param1=value1&param2=value2
    # if fuseki_endpoint is None:
    #     fuseki_endpoint = request.args.get('fuseki_endpoint')
    # if source_id is None:
    #     source_id = request.args.get('source_id')
    app.logger.info("Supplied vars...\nfuseki_endpoint: {}\nsource_id: {}".format(fuseki_endpoint, source_id))
    # app.logger.info("Supplied vars (via request args)...\nfuseki_endpoint: {}\nsource_id: {}".format(request.args.get('fuseki_endpoint'), request.args.get('source_id')))

    ## TODO: Return error when endpoint or source_id not provided
    if fuseki_endpoint is None or source_id is None:
        ## Data passed to this container is from the trusted internal docker application network, so we don't check it again - the api checks the endpoint and source are valid 
        app.logger.error("Route must be provided with a fuseki_endpoint and source_id! Using defaults...")
        fuseki_endpoint = "http://dwh.proxy/fuseki/cometar_live/query"
        source_id = "test"
    result = None
    response = {}
    response['status_code'] = 500
    response['content'] = ""
    result = meta.pull_fuseki_datatree(fuseki_endpoint, source_id)
    response['content'] += "{}\n".format(result)

    ## TODO: Write objects to flat structured CSV files - 1 per table
        ## Filename includes source_id
    all_trees = []
    for tree in result[source_id]:
        all_trees.append(tree.whole_tree_csv())
    combined_tree = meta.combine_csv_trees(all_trees)
    meta.write_csv(combined_tree, source_id, app.config["csv_out_dir"])
    response['content'] += "{}\n".format("CSV written")

    # sql_trees = {"meta": "", "data": ""}
    # app.logger.debug("Looping through results: {}".format(", ".join([x.name for x in result[source_id]])))
    # for top_node in result[source_id]:
    #     node_sql = None
    #     node_sql = top_node.whole_tree_inserts()
    #     # app.logger.debug("node_sql: {}".format(node_sql))
    #     sql_trees["meta"] += "\n".join(node_sql["meta"])
    #     sql_trees["data"] += "\n".join(node_sql["data"])

    # sql_tree0 = result["test"][0].whole_tree_inserts()
    # app.logger.info("## ** ---------- ---------- ---------- ** ##")
    # app.logger.info("All trees as objects:\n{}".format(result[source_id]))
    # app.logger.info("Whole tree SQL inserts:\n{}".format(sql_trees))
    # # app.logger.info("Whole meta SQL:\n{}".format("\n".join(sql_trees["meta"])))
    # # app.logger.info("Whole data SQL:\n{}".format("\n".join(sql_trees["data"])))
    # app.logger.info("Whole meta SQL:\n{}".format(sql_trees["meta"]))
    # app.logger.info("Whole data SQL:\n{}".format(sql_trees["data"]))
    # app.logger.info("## ** ---------- ---------- ---------- ** ##")
    # response['content'] += "{}\n".format("SQL generated")

    ## Write SQL to file
    ## TODO
    db_conn = connection.get_database_connection(
        os.getenv("I2B2DBHOST"),
        os.getenv("I2B2DBNAME"),
        os.getenv("DB_ADMIN_USER"),
        os.getenv("DB_ADMIN_PASS")
    )
    meta.csv_to_database(db_conn=db_conn, out_dir=app.config["csv_out_dir"])

    ## Push sql to i2b2 database
    # meta.sql_to_i2b2(sql_trees["meta"], sql_trees["data"])
    ## TODO: More checks before writing response!
    response['status_code'] = 200
    # response['content'] += "{}\n".format("Database updated")
    app.logger.info("Update complete!")
    app.logger.info(response)
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

@app.route('/test3/', defaults={'a1' : 'd1', 'a2' : 'd2', 'a3' : 'd3'})
def test3(a1,a2,a3):
    app.logger.info("Running test3 route...")
    app.logger.debug("plain 'locals': {}".format(locals()))
    app.logger.debug("'locals.__dir__': {}".format(locals().__dir__()))
    # app.logger.debug("'locals.__dict__': {}".format(locals().__dict__))
    app.logger.debug("'locals.items()': {}".format(locals().items()))
    for param, val in locals().items():
        app.logger.debug("Param '{}': {}".format(param, val))
    app.logger.debug("'a1': {}".format(a1))
    app.logger.debug("'args a1': {}".format(request.args.get('a1')))
    return {"status": 200, "content": "Test complete - see logs"}

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

@app.route('/testfull')
def testfull():
    app.logger.info("Running testfull route...")
    import meta
    result = None
    response = {}
    app.logger.info("Running functions...")
    response['status_code'] = 200
    response['content'] = ""
    result = meta.pull_fuseki_datatree()
    response['content'] += "{}\n".format(result)

    ## TODO: Configure source system - not use hardcoded "test"
    sql_trees = {"meta": "", "data": ""}
    app.logger.debug("Looping through results: {}".format(", ".join([x.name for x in result["test"]])))
    for top_node in result["test"]:
        node_sql = None
        node_sql = top_node.whole_tree_inserts()
        app.logger.debug("node_sql: {}".format(node_sql))
        sql_trees["meta"] += "\n".join(node_sql["meta"])
        sql_trees["data"] += "\n".join(node_sql["data"])

    # sql_tree0 = result["test"][0].whole_tree_inserts()
    app.logger.info("## ** ---------- ---------- ---------- ** ##")
    app.logger.info("All trees as objects:\n{}".format(result["test"]))
    app.logger.info("Whole tree SQL inserts:\n{}".format(sql_trees))
    # app.logger.info("Whole meta SQL:\n{}".format("\n".join(sql_trees["meta"])))
    # app.logger.info("Whole data SQL:\n{}".format("\n".join(sql_trees["data"])))
    app.logger.info("Whole meta SQL:\n{}".format(sql_trees["meta"]))
    app.logger.info("Whole data SQL:\n{}".format(sql_trees["data"]))
    app.logger.info("## ** ---------- ---------- ---------- ** ##")
    response['content'] += "{}\n".format("SQL generated")

    ## Write SQL to file
    ## TODO

    ## Push sql to i2b2 database
    meta.sql_to_i2b2(sql_trees["meta"], sql_trees["data"])
    response['content'] += "{}\n".format("Database updated")
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

