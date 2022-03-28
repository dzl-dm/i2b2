""" meta.py
The section for meta-data processing. Eg from CoMetaR
"""
from flask import current_app as app

import logging
logger = logging.getLogger(__name__)

import csv
from queries import connection
from datetime import datetime as dt
from queries import queries
import model
import os
import psycopg2
import psycopg2.sql
import time
import yaml
# from sqlalchemy import sql as sqlalch

## TODO: Use app context - the yaml file is now loaded there too
# app_settings:dict = {}
# with open(os.getenv("APP_CONF_PATH"), "r") as yaml_file:
#     app_settings = yaml.safe_load(yaml_file)

def prepare_filesystem():
    """Ensure directories exist and old files are cleaned up before processing starts"""
    pass

def pull_fuseki_datatree(fuseki_endpoint:str, source_id:str) -> dict:
    """Pull the full tree, build objects and serialise data"""
    logger.debug("fetching all fuseki data for endpoint (managed by queries module, using config)")
    ## Save the fuseki tree data - could have multiple sources - TODO: naming scheme needs more thought
    metadata_trees:dict = {}
    metadata_trees[source_id] = []
    conn = connection.get_fuseki_connection(fuseki_endpoint, "requests", source_id = source_id)
    top_elements:dict = queries.top_elements(conn)
    for node_uri, node_type in top_elements.items():
        ## Wait to allow connections to be returned by system (NOTE: testing when using sparqlwrapper, which doesn't reuse connections!)
        # time.sleep(125)
        # sparql_wrapper = queries.get_sparql_wrapper(fuseki_endpoint)
        metadata_trees[source_id].append(get_tree(conn, node_uri, node_type))
    ##TEST:
    # check_attrs = ["datatype_xml", "visual_attribute", "concept_long", "test"]
    # for check_attr in check_attrs:
    #     show = "NULL"
    #     if hasattr(metadata_trees[source_id][0], check_attr):
    #         show = getattr(metadata_trees[source_id][0], check_attr)
    #     logger.debug("parent.datatype_xml ({}): {}".format(hasattr(metadata_trees[source_id][0], check_attr), show))
    # logger.debug("dict(parent): {}".format(dict(metadata_trees[source_id][0])))
    # logger.debug("parent.__dict__: {}".format(metadata_trees[source_id][0].__dict__()))
    # # serialise_data(metadata_trees[source_id][0])
    # logger.debug("JSON-ised tree: {}".format(serialise_tree(metadata_trees[source_id][0], output_type="json")))
    # logger.debug("CSV dump of tree: {}".format(serialise_tree(metadata_trees[source_id][0], output_type="csv")))
    return metadata_trees

def pull_fuseki_datatree_sparqlwrapper(fuseki_endpoint:str, source_id:str) -> dict:
    """Pull the full tree, build objects and serialise data"""
    from queries import queries
    logger.debug("fetching all fuseki data for endpoint (managed by queries module, using config)")
    sparql_wrapper = queries.get_sparql_wrapper(fuseki_endpoint)
    top_elements:dict = queries.top_elements(sparql_wrapper)

    ## Save the fuseki tree data - could have multiple sources - TODO: naming scheme needs more thought
    metadata_trees:dict = {}
    metadata_trees[source_id] = []
    for node_uri, node_type in top_elements.items():
        ## Wait to allow connections to be returned by system (NOTE: testing when using sparqlwrapper, which doesn't reuse connections!)
        time.sleep(125)
        # sparql_wrapper = queries.get_sparql_wrapper(fuseki_endpoint)
        metadata_trees[source_id].append(get_tree(sparql_wrapper, node_uri, node_type))
    ##TEST:
    # check_attrs = ["datatype_xml", "visual_attribute", "concept_long", "test"]
    # for check_attr in check_attrs:
    #     show = "NULL"
    #     if hasattr(metadata_trees[source_id][0], check_attr):
    #         show = getattr(metadata_trees[source_id][0], check_attr)
    #     logger.debug("parent.datatype_xml ({}): {}".format(hasattr(metadata_trees[source_id][0], check_attr), show))
    # logger.debug("dict(parent): {}".format(dict(metadata_trees[source_id][0])))
    logger.debug("parent.__dict__: {}".format(metadata_trees[source_id][0].__dict__()))
    # serialise_data(metadata_trees[source_id][0])
    logger.debug("JSON-ised tree: {}".format(serialise_tree(metadata_trees[source_id][0])))
    return metadata_trees

def serialise_data(parent_node:object):
    """Use pickle to save the node objects to file"""
    # import pickle
    # logger.info("Pickling data...")
    # with open('node_data.txt', 'wb') as fh:
    #     pickle.dump(parent_node, fh)

    import json
    logger.info("Serialising data to JSON...")
    with open('node_data.json', 'wb') as fh:
        json.dump(parent_node, fh)

    logger.debug("Done!")
    
def serialise_tree(parent:object, output_type:str = "csv") -> str:
    """Call the __dict__ function of the parent, then all children recursively"""
    if output_type == "csv":
        serialised_data = parent.whole_tree_csv()
    elif output_type == "json":
        json_tree = parent.__dict__()
        json_tree["child_nodes"] = serialise_children(parent)
        serialised_data = json_tree
    return serialised_data

def serialise_children(node:object) -> str:
    """Call the __dict__ function of the node and use as JSON"""
    child_trees = []
    if node.child_nodes is not None and len(node.child_nodes) > 0:
        for child in node.child_nodes:
            child_tree = child.__dict__()
            child_tree["child_nodes"] = serialise_children(child)
            child_trees.append(child_tree)
    return child_trees

# def get_tree(sparql_wrapper, node_uri:str = "<http://data.dzl.de/ont/dwh#Patientdata>", node_type:str = "concept", parent_node:object = None):
# def get_tree(sparql_wrapper, node_uri:str = "<http://data.dzl.de/ont/dwh#naturalParent>", node_type:str = "modifier", parent_node:object = None):
# def get_tree(sparql_wrapper, node_uri:str = "<http://data.dzl.de/ont/dwh#HealthQuest>", node_type:str = "concept", parent_node:object = None):
def get_tree(conn, node_uri:str, node_type:str, parent_node:object = None):
    """Get all children under a single node"""
    from queries import queries
    node_uri = node_uri.strip("<>")
    new_parent = _element(conn, node_uri, node_type, parent_node = parent_node, )
    children = queries.getChildren(conn, node_uri)
    for node_uri, node_type in children.items():
        get_tree(conn, node_uri, node_type, new_parent)
    # data = {"status_code": 200, "content": new_parent}
    return new_parent

def get_element(node_uri:str = "<http://data.dzl.de/ont/dwh#PHQ4>", node_type:str = "concept") -> dict:
    """Get a single node - with all its details"""
    logger.info("Fetching the node data for '{}' and writing SQL".format(node_uri))
    import datetime
    from queries import queries
    # import i2b2_sql

    dt_string = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    
    node_uri = node_uri.strip("<>")
    element:dict = {}
    element["node_type"] = node_type ## Would be available from the top_elements or child_elements queries
    element["name"] = queries.getName(node_uri)
    element["label"] = {queries.getLabel(node_uri): "en"}
    element["displayLabel"] = {queries.getDisplayLabel(node_uri): "en"}
    element["notations"] = queries.getNotations(node_uri)
    element["display"] = queries.getDisplay(node_uri)
    element["datatypeXml"] = queries.getDatatypeXml(node_uri, dt_string)
    element["datatype"] = queries.getDatatypeRaw(node_uri)
    element["description"] = {queries.getDescription(node_uri): "en"}
    element["children"] = queries.getChildren(node_uri)
    logger.debug("Node data: {}".format(element))

    from model import MetaNode
    new_elem = MetaNode.MetaNode(
        node_uri = node_uri,
        name = element["name"],
        node_type = element["node_type"],
        pref_labels = element["label"],
        display_labels = element["displayLabel"],
        notations = element["notations"],
        display_status = element["display"],
        datatype = element["datatype"],
        descriptions = element["description"]
    )
    logger.info(new_elem.meta_inserts)
    logger.info(new_elem.data_inserts)
    # i2b2_sql.write_sql_for_node()
    return element

def _element(conn, node_uri:str = "<http://data.dzl.de/ont/dwh#PHQ4>", node_type:str = "concept", parent_node:object = None) -> object:
    """Get a single node - with all its details"""
    logger.info("Fetching the node data for '{}'".format(node_uri))
    from queries import queries

    node_uri = node_uri.strip("<>")
    element:dict = queries.getAttributes(conn, node_uri)
    logger.debug("Node data: {}".format(element))

    logger.debug("element[\"notations\"]: {}".format(element["notations"]))
    from model import MetaNode
    new_elem = MetaNode.MetaNode(
        node_uri = node_uri,
        name = element["name"],
        node_type = node_type,
        parent_node = parent_node,
        pref_labels = {element["prefLabel"]: "en"},
        display_labels = {element["displayLabel"]: "en"},
        notations = element["notations"],
        display_status = element["display"],
        datatype = element["datatype"],
        units = element["units"],
        descriptions = {element["description"]: "en"},
        sourcesystem_cd = conn["source_id"]
    )
    return new_elem

def sql_to_i2b2(meta_statements:str, data_statements:str):
    """Run the SQL against the i2b2 postgres database to insert the rules"""
    result = False
    logger.debug("Connection to db...")
    try:
        ## TODO: get details from env/settings
        conn = psycopg2.connect (
            dbname = "i2b2",
            user = "i2b2",
            host = "i2b2.database",
            password = "ChangeMe"
        )
        logger.info("Connection to postgres successful! {}".format(conn))
        cursor = conn.cursor()
        logger.debug("Cursor created! {}".format(cursor))
    except:
        logger.warn("Connection to postgres UN-successful! {}".format(conn))
        conn = None

    ## TODO: Read from file/config
    meta_preamble = """DELETE FROM i2b2metadata.table_access WHERE c_table_cd LIKE 'i2b2_%';
        DELETE FROM i2b2metadata.i2b2 WHERE sourcesystem_cd='test';"""
    data_preamble = """DELETE FROM i2b2demodata.concept_dimension WHERE sourcesystem_cd='test';
        DELETE FROM i2b2demodata.modifier_dimension WHERE sourcesystem_cd='test';"""
    if conn is not None:
        try:
            logger.info("Metadata import into i2b2 server (part 1/2)...")
            sql_object = psycopg2.sql.SQL(
                "{}\n{}".format(meta_preamble, meta_statements)
            )
            cursor.execute( sql_object )
            logger.info("Metadata import into i2b2 server (part 2/2)...")
            sql_object = psycopg2.sql.SQL(
                "{}\n{}".format(data_preamble, data_statements)
            )
            cursor.execute( sql_object )
            logger.info("New data uploaded! (Status: {})".format(cursor.statusmessage))
            
            logger.info("Updating patient count...")
            with open('patient_count.sql','r') as sql_file:
                cursor.execute(sql_file.read())
            logger.debug("Done")
            ## Everything has worked, so commit transactions to PostgreSQL
            conn.commit()
            result = True
        except Exception as err:
            logger.warn("cursor.execute() ERROR:", err)
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
        return result

def insert_test_demodata():
    """Check I'm talking to the database correctly"""
    new_demodata = model.Concept("test\path", "test code", "Test code name", dt.now(), dt.now(), dt.now(), "TEST", None)
    model.db.session.add(new_demodata)  # Adds new User record to database
    model.db.session.commit()
    ## Basic execution:
    # result = model.db.engine.execute(sqlalch.text(sql_data).execution_options(autocommit=True))

    ## OR maybe I need to use it like this... api > 1.4
    # with model.db.engine.connect() as connection:
    #     result = connection.execute(sqlalch.text(sql_data))    

    ## psycopg2 directly (without sqlalchemy)
    pass

def insert_sql_test():
    """Test running sql file against database"""
    logger.info("Attempting to run an sql file via sqlalchemy")
    # test_file = "/var/tmp/meta/i2b2-sql/meta.sql"
    # test_file = "/var/tmp/meta/i2b2-sql/data.sql"
    # with open(test_file, 'r') as file:
    #     sql_data = file.read()
    # sql_data = """
    # INSERT INTO i2b2demodata.concept_dimension ( concept_path,concept_cd,name_char,update_date, download_date,import_date,sourcesystem_cd )
    # VALUES( '\\i2b2\\http:/data.custom.de/ont/dwh#DemoData\\http:/data.custom.de/ont/dwh#SubCategory\\','CODESYS:CAT1; ALTCODESYS:CAT_A','Sub Category 1',current_timestamp, '2022-03-17 17:20:21.0',current_timestamp,'test' );
    # INSERT INTO i2b2demodata.modifier_dimension ( concept_path,concept_cd,name_char,update_date, download_date,import_date,sourcesystem_cd )
    # VALUES( '\\S:65656005\\','S:65656005','Natural mother',current_timestamp, '2022-03-17 17:20:21.0',current_timestamp,'test' );
    # INSERT INTO i2b2demodata.modifier_dimension ( concept_path,concept_cd,name_char,update_date, download_date,import_date,sourcesystem_cd )
    # VALUES( '\\S:9947008\\','S:9947008','Natural father',current_timestamp, '2022-03-17 17:20:21.0',current_timestamp,'test' );
    # INSERT INTO i2b2demodata.concept_dimension ( concept_path,concept_cd,name_char,update_date, download_date,import_date,sourcesystem_cd )
    # VALUES( '\\i2b2\\http:/data.custom.de/ont/dwh#DemoData\\http:/data.custom.de/ont/dwh#SubCategory2\\','CODESYS:CAT2','Sub Category 2',current_timestamp, '2022-03-17 17:20:21.0',current_timestamp,'test' );
    # """
    sql_data = """
    DELETE FROM i2b2demodata.concept_dimension WHERE sourcesystem_cd='test';
    DELETE FROM i2b2demodata.modifier_dimension WHERE sourcesystem_cd='test';

INSERT INTO i2b2demodata.concept_dimension ( concept_path,concept_cd,name_char,update_date, download_date,import_date,sourcesystem_cd ) VALUES( '\i2b2\:DemoData\:SubCategory2\','CODESYS:CAT2','Sub Category 2',current_timestamp, '2022-03-18 10:35:06.0',current_timestamp,'test' );
INSERT INTO i2b2demodata.concept_dimension ( concept_path,concept_cd,name_char,update_date, download_date,import_date,sourcesystem_cd ) VALUES( '\i2b2\:DemoData\:SubCategory\','CODESYS:CAT1; ALTCODESYS:CAT_A','Sub Category 1',current_timestamp, '2022-03-18 10:35:06.0',current_timestamp,'test' );
INSERT INTO i2b2demodata.modifier_dimension ( modifier_path,modifier_cd,name_char,update_date, download_date,import_date,sourcesystem_cd ) VALUES( '\S:9947008\','S:9947008','Natural father',current_timestamp, '2022-03-18 10:35:06.0',current_timestamp,'test' );
INSERT INTO i2b2demodata.modifier_dimension ( modifier_path,modifier_cd,name_char,update_date, download_date,import_date,sourcesystem_cd ) VALUES( '\S:65656005\','S:65656005','Natural mother',current_timestamp, '2022-03-18 10:35:06.0',current_timestamp,'test' );
    """
    logger.debug("Running sql against db: {}".format(sql_data))

    ## Basic execution:
    # result = model.db.engine.execute(sqlalch.text(sql_data).execution_options(autocommit=True))

    ## OR maybe I need to use it like this... api > 1.4
    # with model.db.engine.connect() as connection:
    #     result = connection.execute(sqlalch.text(sql_data))    

    ## psycopg2 directly (without sqlalchemy)

    try:
        # declare a new PostgreSQL connection object
        conn = psycopg2.connect (
            dbname = "i2b2",
            user = "i2b2",
            host = "i2b2.database",
            password = "ChangeMe"
        )
        logger.info("Connection to postgres successful! {}".format(conn))
        # attempt to create a cursor object
        cursor = conn.cursor()
        logger.info("Cursor created! {}".format(cursor))
    except:
        logger.warn("Connection to postgres UN-successful! {}".format(conn))

    try:
        # have sql.SQL() return a sql.SQL object
        sql_object = psycopg2.sql.SQL(
            # pass SQL string to sql.SQL()
            sql_data
        # ).format(
        #     # pass the identifier to the Identifier() method
        #     psycopg2.sql.Identifier( table_name )
        )

        # pass the psycopg2.sql.SQL object to execute() method
        cursor.execute( sql_object )

        # print message if no exceptions were raised
        logger.info("cursor.execute() FINISHED")

        # use the fetchall() method to return a list of all the data
        logger.info("cursor.statusmessage: {}".format(cursor.statusmessage))
        # table_data = cursor.fetchall()

        # # enumerate over the list of tuple rows
        # for num, row in enumerate(table_data):
        #     print ("row ({}): {}".format(num, row))
        #     print (type(row))
        #     print ("\n")

    except Exception as err:
        logger.warn("cursor.execute() ERROR:", err)

        # rollback the statement
        conn.rollback()

    # commit transactions to PostgreSQL
    conn.commit()

    # close the cursor object to avoid memory leaks
    cursor.close()

    # close the connection object
    conn.close()
    result = True
    logger.info("Result from running sql ({}):\n{}".format(sql_data, result))
    return result

def combine_csv_trees(trees:list) -> dict:
    """Combine multiple CSV trees with the same schema/table structure"""
    combined_tree = None
    first = True
    for tree in trees:
        if first:
            first = False
            combined_tree = tree
        else:
            for schema, tables in tree:
                for table, data in tables:
                    combined_tree[schema][table].extend(data)
    return combined_tree

def write_csv(csv_tree:dict, sourcesystem_id:str, out_dir, delim:str = ";"):
    """Take a dict which has a list of lists for each table - write each dict entry as a csv file"""
    logger.debug("Writing csv_tree to files under '{}': {}".format(out_dir, csv_tree))
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    for schema_name, tables in csv_tree.items():
        for table_name, data_structure in tables.items():
            filename = os.path.join(out_dir, "{sourcesystem_id}.{schema_name}.{table_name}.csv".format(sourcesystem_id=sourcesystem_id, schema_name=schema_name, table_name=table_name))
            with open(filename, 'w') as f: 
                write = csv.writer(f, delimiter = delim)
                write.writerows(data_structure)

def csv_to_database(db_conn, out_dir:str = "/var/metadata-translation/"):
    """Adjust CSV data to include elements like sourcesystem_cd, then add to database"""
    ## Get list of sources to be updated (by reading CSV filenames?)
    db_prepared_prefix = "dbready."
    for file_name in os.listdir(out_dir):
        if db_prepared_prefix in file_name:
            logger.error("Files already processed, stopping")
            return None
    base_files = [f for f in os.listdir(out_dir) if os.path.isfile(os.path.join(out_dir, f))]
    logger.debug("CSV file list: {}".format(base_files))
    sources = list(set([s.split(".")[0] for s in base_files]))
    logger.debug("Updating sources: {}".format(sources))

    ## TODO: Read CSV data to memory?
    ## TODO: Clean CSV (write to new file)
    for csv_file in base_files:
        current_source = csv_file.split(".")[0]
        current_schema = csv_file.split(".")[1]
        current_table = csv_file.split(".")[2]
        ## TODO: get dict of cols and length so we can update and trim
        columns = col_limits(db_conn, current_schema, current_table)
        prepared_file = open(os.path.join(out_dir, "{}{}".format(db_prepared_prefix, csv_file), 'w'))
        with open(os.path.join(out_dir, csv_file), 'r') as base:
            reader = csv.DictReader(base, fieldnames=fields)
            pass
    prepared_files = [f for f in os.listdir(out_dir) if os.path.isfile(os.path.join(out_dir, f)) and db_prepared_prefix in f]

    ## TODO: Clean i2b2 data from sources to be updated
    ## TODO: Push csv data to database
    ## TODO: Commit/rollback

    for csv_file in prepared_files:
        current_source = csv_file.split(".")[1]
        current_schema = csv_file.split(".")[2]
        current_table = csv_file.split(".")[3]
        with open(os.path.join(out_dir, csv_file), 'r') as f:
            reader = csv.reader(f)
            data = next(reader) 
            query = 'insert into {schema}.{table} values ({values})'
            query = query.format(
                schema = current_schema,
                table = current_table,
                values = ','.join('?' * len(data))
            )
            cursor = db_conn.cursor()
            cursor.execute(query, data)
            for data in reader:
                cursor.execute(query, data)
            cursor.commit()
    pass

def col_limits(db_conn, schema, table) -> dict:
    """List columns with length limits"""
    cursor = db_conn.cursor()
    cursor.execute('SELECT * FROM {}.{};'.format(schema, table))
    tmp = cursor.fetchall()

    # Extract the column names
    col_names = []
    for elt in cursor.description:
        col_names.append(elt[0])

    logger.debug(col_names)
    # Create the dataframe, passing in the list of col_names extracted from the description
    # df = pd.DataFrame(tmp, columns=col_names)
    pass

def shorten_csv_data():
    """Using defined limits, ensure csv data will fit into the database"""
    pass

def add_source():
    """Add the sourcesystem_cd to each csv data line"""
    pass

# class database_connection(object):
#     def __init__():
#         conn = None
#         try:
#             conn = psycopg2.connect("dbname = 'routing_template' user = 'postgres' host = 'localhost' password = '****'")
#         except (psycopg2.DatabaseError, ex):
#             logger.error("I am unable to connect the database: {}".format(ex))
#             return False
#         pass

