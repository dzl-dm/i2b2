""" meta.py
The section for meta-data processing. Eg from CoMetaR
"""
from cmath import log
from flask import current_app as app

import logging
logger = logging.getLogger(__name__)

import csv
import datetime
from queries import connection
from datetime import datetime as dt
from queries import queries
import model
import os
import psycopg2
import psycopg2.sql
import time

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
        dwh_display_status = element["display"],
        datatype = element["datatype"],
        units = element["units"],
        descriptions = {element["description"]: "en"},
        sourcesystem_cd = conn["source_id"]
    )
    return new_elem

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
    logger.info("Generating '{}'-tpye serialised output for tree...".format(output_type))
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

def combine_csv_trees(trees:list) -> dict:
    """Combine multiple CSV trees with the same schema/table structure"""
    combined_tree = None
    first = True
    for tree in trees:
        if first:
            first = False
            combined_tree = tree
            logger.debug("Added first tree to combined trees: {}".format(tree))
        else:
            for schema, tables in tree.items():
                for table, data in tables.items():
                    # logger.debug("Adding items to combined trees from table '{}.{}': {}".format(schema, table, data))
                    combined_tree[schema][table].extend(data)
    return combined_tree

def write_csv(csv_tree:dict, sourcesystem_id:str, out_dir:str, delim:str = ";"):
    """Take a dict which has a list of lists for each table - write each dict entry as a csv file"""
    logger.debug("Writing csv_tree to files under '{}': {}".format(out_dir, csv_tree))
    try:
        if not os.path.isdir(out_dir):
            os.makedirs(out_dir)
        for schema_name, tables in csv_tree.items():
            for table_name, data_structure in tables.items():
                filename = os.path.join(out_dir, "{sourcesystem_id}.{schema_name}.{table_name}.csv".format(sourcesystem_id=sourcesystem_id, schema_name=schema_name, table_name=table_name))
                with open(filename, 'w') as f: 
                    write = csv.writer(f, delimiter = delim)
                    write.writerows(data_structure)
        return True
    except:
        logger.error("Failed to write CSV data: {}".format(Exception))
        return False

def csv_to_database(db_conn, out_dir:str, delim:str = ";", sources:list = None):
    """Adjust CSV data to include elements like sourcesystem_cd, then add to database"""
    ## Get list of sources to be updated (by reading CSV filenames?)
    db_prepared_prefix = "dbready."
    possible_files = os.listdir(out_dir)

    # if sources is None:
        ## Assume all sources which exist in the processing directory
        # for file_name in possible_files:
        #     # if db_prepared_prefix in file_name:
        #     #     logger.error("Files already processed, stopping")
        #     #     return None
        #     if db_prepared_prefix in file_name:
        #         logger.warn("Files already processed, continuing anyway...")
    if sources is None:
        sources = list(set([f.split(".")[0] for f in possible_files if not f.startswith(db_prepared_prefix) and os.path.isfile(os.path.join(out_dir, f))]))
        logger.debug("Updating sources: {}".format(sources))
    base_files = [f for f in possible_files if f.split(".")[0] in sources]
    logger.debug("CSV file list: {}".format(base_files))

    ## Update col limits
    type_limits = app.config["i2b2db_col_limits"]
    if type_limits:
        for current_schema, current_tables in type_limits.items():
            if current_tables:
                for current_table, table_limits in current_tables.items():
                    update_col_limits(db_conn, current_schema, current_table, table_limits)
                    # logger.debug("Col limits after updating: {}".format(_get_col_limits(db_conn, current_schema, current_table)))

    ## Clean CSV (write to new file)
    for csv_file in base_files:
        current_source = csv_file.split(".")[0]
        current_schema = csv_file.split(".")[1]
        current_table = csv_file.split(".")[2]
        ## get dict of cols and length so we can update and trim
        col_limits = _get_col_limits(db_conn, current_schema, current_table)
        with open(os.path.join(out_dir, csv_file), 'r') as base, open(os.path.join(out_dir, "{}{}".format(db_prepared_prefix, csv_file)), 'w') as prepared_file:
            reader = csv.DictReader(base, fieldnames=list(col_limits.keys()), skipinitialspace=True, delimiter = delim)
            writer = csv.DictWriter(prepared_file, fieldnames=list(col_limits.keys()))
            for row in reader:
                new_row, changed = shorten_csv_data(row, col_limits, current_schema, current_table)
                if changed:
                    logger.debug("Updating row...\nold: {}\nnew:{}".format(row, new_row))
                new_row, changed = add_source(new_row, current_source, current_schema, current_table)
                logger.debug("Writing prepared row: {}".format(new_row))
                writer.writerow(new_row)
    prepared_file_names = [f for f in os.listdir(out_dir) if os.path.isfile(os.path.join(out_dir, f)) and f.startswith(db_prepared_prefix) and f.split(".")[1] in sources]
    logger.info("Prepared CSV files in '{}'... {}".format(out_dir, prepared_file_names))

    ## Clean i2b2 data from sources to be updated and write new data
    try:
        ## DELETE statements...
        if clean_sources_in_database(db_conn, sources):
            prepared_file_paths = [os.path.join(out_dir, x) for x in prepared_file_names]
            ## Push csv data to database
            if _push_prepared_csv_to_database(db_conn, prepared_file_paths):
                ## Commit only if delete and insert stages are successful
                logger.debug("Commiting to database...")
                db_conn.commit()
            else:
                db_conn.rollback()
                logger.error("Failed to complete database INSERTs")
        else:
            db_conn.rollback()
            logger.error("Failed to complete database DELETEs, not proceding with inserts...")
    except Exception as e:
        db_conn.rollback()
        logger.error("Failed to complete database update...\n{}".format(e))
    finally:
        db_conn.close()
    ## TODO: Clean up CSV files?

    return True

def prepare_custom_queries(source_id:str, source_dir:str, delim = ";"):
    """Ensure all the columns which are needed are populated
    File must have header line"""
    if not os.path.isdir(source_dir):
        logger.error("Source dir ({}) does not exist - cannot process further".format(source_dir))
        return False
    db_prepared_prefix = "dbready."
    possible_files = os.listdir(source_dir)
    base_files = [x for x in possible_files if x.split(".")[0] == source_id]
    logger.debug("Base files: {}".format(base_files))
    out_dir = "/tmp/"
    for csv_file in base_files:
        # current_source = csv_file.split(".")[0]
        current_schema = csv_file.split(".")[1]
        current_table = csv_file.split(".")[2]
        ## get dict of cols and length so we can update and trim
        with open(os.path.join(source_dir, csv_file), 'r') as base, open(os.path.join(out_dir, "{}{}".format(db_prepared_prefix, csv_file)), 'w') as prepared_file:
            reader = csv.DictReader(base, skipinitialspace=True, delimiter = delim)
            first = True
            for row in reader:
                if first:
                    first = False
                    headers = list(row.keys())
                    logger.debug("Headers: {}".format(headers))
                    logger.debug("For schema.table: {}.{}".format(current_schema, current_table))
                    if "sourcesystem_cd" not in headers and "{}.{}".format(current_schema, current_table) != "i2b2metadata.table_access":
                        ##TODO: Make condition check database columns for the current table - rather than hard coding a specific case
                        ## If the table should have "sourcesystem_cd" column, add it
                        ## TODO: Otherwise add it somehow, eg in c_table_cd for table_access
                        headers.append("sourcesystem_cd")
                        logger.debug("Added sourcesystem_cd to headers...")
                    writer = csv.DictWriter(prepared_file, fieldnames=headers)
                    writer.writeheader()

                new_row = row
                if "sourcesystem_cd" in headers:
                    new_row["sourcesystem_cd"] = source_id
                if "sourcesystem_cd" not in headers and "{}.{}".format(current_schema, current_table) == "i2b2metadata.table_access":
                    new_row["c_table_cd"] = "{}_{}".format(source_id, new_row["c_table_cd"])
                    logger.debug("Adjusted c_table_cd to include sourcesystem_cd: {}".format(source_id))
                logger.debug("Writing prepared row: {}".format(new_row))
                writer.writerow(new_row)
    prepared_file_paths = [os.path.join(out_dir, f) for f in os.listdir(out_dir) if os.path.isfile(os.path.join(out_dir, f)) and f.startswith(db_prepared_prefix) and f.split(".")[1] == source_id]
    logger.info("Prepared CSV files in '{}'... {}".format(out_dir, prepared_file_paths))
    return prepared_file_paths

def update_col_limits(db_conn, schema:str, table:str, limits:dict = None) -> bool:
    """Set limits for any defined cols the schema/table
    :param limits: {col_name: col_type} - where an entry exists in this dict, it will be updated
    """
    logger.info("Updating col datatype and limits for '{}.{}'...\n{}".format(schema, table, limits))
    cursor = db_conn.cursor()
    result = False
    if limits:
        try:
            for col_name, col_type in limits.items():
                cursor.execute("ALTER TABLE {}.{} ALTER COLUMN {} TYPE {};".format(schema, table, col_name, col_type))
                logger.debug("Ran ALTER query: {}".format(cursor.query))
            db_conn.commit()
            result = True
        except Exception as e:
            ## TODO: This can fail if an existing entry is too long - we download data, update limits, trim data and re-upload
            logger.error("Failed to update limits for '{}.{}': {}\n{}".format(schema, table, limits, e))
            db_conn.rollback()
        finally:
            cursor.close()
    return result

def _get_col_limits(db_conn, schema:str, table:str) -> dict:
    """Get limits for all cols in the schema/table"""
    logger.debug("Getting cols for '{}.{}'...".format(schema, table))
    cursor = db_conn.cursor()
    cursor.execute("SELECT column_name, data_type, character_maximum_length AS max_length FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}';".format(schema, table))
    tmp = cursor.fetchall()
    # logger.debug("Fetchall: {}".format(tmp))
    # logger.debug("cursor.__dir__: {}".format(cursor.__dir__()))

    cols = {}
    for col in tmp:
        # logger.debug("col: {}".format(col))
        cols[col[0]] = col[1]
        if col[1] == "character varying" and type(col[2]) is int:
            cols[col[0]] += "({})".format(col[2])

    logger.debug(cols)
    return cols

def shorten_csv_data(row:dict, col_limits:dict, schema = None, table = None) -> list[dict, bool]:
    """Using defined limits, ensure csv data will fit into the database"""
    new_row = {}
    changed = False
    # logger.debug("Working on row: {}".format(row))
    for col_name, col_data in row.items():
        # logger.debug("Working on col: {} - {}".format(col_name, col_data))
        if col_name in col_limits and col_data:
            ## Check col length, trim if necessary
            if col_limits[col_name].startswith("character varying"):
                max_col_length = int(col_limits[col_name].split("(")[-1].rstrip(")"))
                if len(col_data) > max_col_length:
                    logger.info("**Trimming length of field! ({}.{} {} - {})".format(schema, table, col_name, col_data))
                    col_data = "{}...".format(col_data[ : max_col_length-3])
                    changed = True
        new_row[col_name] = col_data
    return new_row, changed

def add_source(row:dict, source_id:str, schema = None, table = None) -> list[dict, bool]:
    """Add the sourcesystem_cd to each csv data line
    :param row: csv dictreader row
    return the row (updated or the same), bool = True if updated (most lines should have a source id!)
    """
    new_row = row
    changed = False
    if "sourcesystem_cd" in row:
        new_row["sourcesystem_cd"] = source_id
        changed = True
    elif schema and schema == "i2b2metadata" and table and table == "table_access" and "c_table_cd" in row:
        ## Add to table_access c_table_cd
        new_row["c_table_cd"] = row["c_table_cd"].replace("i2b2_", "i2b2_{}_".format(source_id))
    else:
        new_row = row
    return new_row, changed

def clean_sources_in_database(db_conn, source_ids:list):
    """DELETE selectively based on the source_ids"""
    ## TODO: Get prepared query from file
    ## TODO: Should this be in a different module?
    translator_deletes = """
        DELETE FROM i2b2metadata.table_access WHERE c_table_cd LIKE %s;
        DELETE FROM i2b2metadata.i2b2 WHERE sourcesystem_cd=%s;
        DELETE FROM i2b2demodata.concept_dimension WHERE sourcesystem_cd=%s;
        DELETE FROM i2b2demodata.modifier_dimension WHERE sourcesystem_cd=%s;
    """
    try:
        cursor = db_conn.cursor()
        for source_id in source_ids:
            cursor.execute(translator_deletes, ["i2b2_{}%".format(source_id), *[source_id] * 3])
        logger.debug("DELETEd source_ids: {}\n{}".format(source_ids, cursor.query))
        return True
    except Exception as e:
        db_conn.rollback()
        logger.error("Failed to complete database DELETEs...\n{}".format(e))
        return False

def _push_prepared_csv_to_database(db_conn, prepared_file_paths:list, delim:str = ","):
    """Push any csv data which is prepared for the database
    Sniffs for header line so should work with or without heading line - using database columns if no header
    :param prepared_file_paths: list of full filepaths
    ## TODO: Work with unknown delimiters (mostly , or ;)? Or always with ,?
    """
    if prepared_file_paths and len(prepared_file_paths) > 0:
        upload_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.0")
        try:
            cursor = db_conn.cursor()
            for csv_file in prepared_file_paths:
                # current_source = csv_file.split(".")[1]
                current_schema = csv_file.split(".")[2]
                current_table = csv_file.split(".")[3]
                with open(csv_file, 'r') as f:
                    ## Check for header line and reset to start of file
                    headers = None
                    has_header = False
                    reader = csv.reader(f, delimiter = delim)
                    first_line = next(reader)
                    logger.debug("Checking headers in file '{}' - first line: {}".format(csv_file, first_line))
                    non_header_test = ["", None, "NULL", "Null"]
                    if not any(col in non_header_test or col.isnumeric() for col in first_line):
                        has_header = True
                        logger.debug("Looks like a HEADER line")
                    if has_header:
                        headers = first_line
                    else:
                        ## Get headers as database columns
                        headers = list(_get_col_limits(db_conn, current_schema, current_table).keys())
                    logger.debug("headers: {}".format(headers))
                    query = 'insert into {schema}.{table}({headers}) values ({values}) ON CONFLICT DO NOTHING;'
                    query = query.format(
                        schema = current_schema,
                        table = current_table,
                        headers = ",".join(headers),
                        values = ','.join(['%s'] * len(headers))
                    )
                    logger.debug("prepared query: {}".format(query))
                    f.seek(0)

                    reader = csv.DictReader(f, fieldnames = headers, delimiter = delim)
                    if has_header:
                        next(reader)
                    for data in reader:
                        # logger.debug("data line: {}".format(data))
                        ## Ensure its an sql null not a string 'null' - int's don't like strings
                        data = {k:(None if v == '' or v == 'NULL' else v) for k,v in data.items()}
                        if "c_dimcode" in data and data["c_dimcode"] is None:
                            ## Hacky fix for NULL and "NULL" being the same once the csv is loaded!
                            logger.debug("c_dimcode is None (changing to 'NULL'): {}".format(data["c_dimcode"]))
                            data["c_dimcode"] = "NULL"
                        data = {k:(upload_time if v == 'current_timestamp' else v) for k,v in data.items()}
                        if "import_date" in data:
                            data["import_date"] = upload_time
                        logger.debug("CSV data: {}".format(data))
                        cursor.execute(query, list(data.values()))
            logger.debug("INSERTed values for '{}.{}'".format(current_schema, current_table))
            return True
        except Exception as e:
            db_conn.rollback()
            logger.error("Failed to complete database INSERTs...\n{}".format(e))
            return False

# class database_connection(object):
#     def __init__():
#         conn = None
#         try:
#             conn = psycopg2.connect("dbname = 'routing_template' user = 'postgres' host = 'localhost' password = '****'")
#         except (psycopg2.DatabaseError, ex):
#             logger.error("I am unable to connect the database: {}".format(ex))
#             return False
#         pass

