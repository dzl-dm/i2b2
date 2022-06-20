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
from datetime import date, datetime as dt
from queries import queries
import model
import os
import psycopg2
import psycopg2.sql
import time
from typing import Tuple

## TODO: Make a "source" class for these functions?
def source_info(source_id:str) -> Tuple[str, str, list[str], dt]:
    """Search for the source_id in the possible sources (eg fuseki or local files)"""
    source_type = _source_type(source_id)
    source_dir, source_file_paths = _source_location(source_id, source_type)
    source_update = _source_update(source_file_paths)
    logger.debug("Source info for id '{}': {},{},{},{}".format(source_id, source_type, source_dir, source_file_paths, source_update))
    time.sleep(1.6)
    return source_type, source_dir, source_file_paths, source_update

def _source_type(source_id:str) -> str:
    """Search for the source_id in the possible sources (eg fuseki or local files)"""
    ## TODO: Use source_type enum?
    ## TODO: Use config for type_name and check function etc
    source_type = "unknown"
    if source_id in app.config["fuseki_sources"].keys():
        source_type = "fuseki"
    elif source_id in os.listdir(app.config["local_file_sources"]):
        source_type = "local_files"
    return source_type

def _source_location(source_id:str, source_type:str) -> Tuple[str, list[str]]:
    """Check if the source has local files and return directory and each file path"""
    dynamic_sources = ["fuseki"] ## TODO: Get from config
    source_dir = None
    source_file_paths = None
    if source_type in dynamic_sources:
        logger.debug("looking for dynamic file data for source: {}".format(source_id))
        source_dir = os.path.join(app.config["dynamic_metadata_directory"], source_id)
        if os.path.isdir(source_dir):
            source_file_paths = [os.path.join(source_dir, f) for f in os.listdir(source_dir) if os.path.isfile(os.path.join(source_dir, f)) and f.startswith(source_id)]
    elif source_type == "local_files":
        logger.debug("looking for manual file data for source: {}".format(source_id))
        ## TODO: implement (using config "local_file_sources" dir)
        test_dir = os.path.join(app.config["local_file_sources"], source_id)
        if os.path.isdir(test_dir):
            source_dir = test_dir
        if source_dir and os.path.isdir(source_dir):
            source_file_paths = [os.path.join(source_dir, f) for f in os.listdir(source_dir) if os.path.isfile(os.path.join(source_dir, f)) and f.endswith(".csv")]
    return source_dir, source_file_paths

def _source_update(source_file_paths:list[str]) -> dt:
    """When was the most recent change to the source files"""
    last_update = None
    ## TODO: If source_file_paths not None, stat each file and find most recent date
    return last_update

def pull_fuseki_datatree(fuseki_endpoint:str, source_id:str) -> dict:
    """Pull the full tree, build objects and serialise data"""
    logger.debug("fetching all fuseki data for endpoint (managed by queries module, using config)")
    ## Save the fuseki tree data - could have multiple sources - TODO: naming scheme needs more thought
    metadata_trees:dict = {}
    metadata_trees[source_id] = []
    conn = connection.get_fuseki_connection(fuseki_endpoint, "requests", source_id = source_id)
    top_elements:dict = queries.top_elements(conn)
    for node_uri, node_type in top_elements.items():
        metadata_trees[source_id].append(get_tree(conn, node_uri, node_type))
    return metadata_trees

def get_tree(conn, node_uri:str, node_type:str, parent_node:object = None):
    """Get all children under a single node"""
    from queries import queries
    node_uri = node_uri.strip("<>")
    new_parent = _element(conn, node_uri, node_type, parent_node = parent_node, )
    children = queries.getChildren(conn, node_uri)
    for node_uri, node_type in children.items():
        get_tree(conn, node_uri, node_type, new_parent)
    return new_parent

def _element(conn, node_uri:str, node_type:str = "concept", parent_node:object = None) -> object:
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

def combine_csv_trees(trees:list) -> dict:
    """Combine multiple CSV trees with the same schema/table structure"""
    logger.debug("Combining CSV trees...")
    combined_tree = None
    first = True
    for tree in trees:
        if first:
            first = False
            combined_tree = tree
            logger.debug("Added first tree to combined trees: {}".format(len(tree)))
        else:
            for schema, tables in tree.items():
                for table, data in tables.items():
                    # logger.debug("Adding items to combined trees from table '{}.{}': {}".format(schema, table, data))
                    combined_tree[schema][table].extend(data)
    return combined_tree

def write_csv(csv_tree:dict, sourcesystem_id:str, out_dir:str, output_delim:str = ","):
    """Take a dict which has a list of lists for each table - write each dict entry as a csv file"""
    logger.debug("Writing csv_tree to files under '{}': (length {})".format(out_dir, len(csv_tree)))
    try:
        if not os.path.isdir(out_dir):
            os.makedirs(out_dir)
        for schema_name, tables in csv_tree.items():
            for table_name, data_structure in tables.items():
                filename = os.path.join(out_dir, "{sourcesystem_id}.{schema_name}.{table_name}.csv".format(sourcesystem_id=sourcesystem_id, schema_name=schema_name, table_name=table_name))
                logger.debug("Writing csv data to file: {}".format(filename))
                with open(filename, 'w') as f: 
                    write = csv.writer(f, delimiter = output_delim)
                    write.writerows(data_structure)
        return True
    except Exception as e:
        logger.error("Failed to write CSV data: {}".format(e))
        return False

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

def shorten_csv_data(row:dict, col_limits:dict, schema = None, table = None) -> tuple[dict, bool]:
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

def add_source(row:dict, source_id:str, schema = None, table = None) -> tuple[dict, bool]:
    """Add the sourcesystem_cd to each csv data line
    :param row: csv dictreader row
    return the row (updated or the same), bool = True if updated (most lines should have a source id!)
    """
    ## TODO: The csv data can ommit columns like sourcesystem_cd when the database should have it - then we don't pick it up when it needs injecting
    new_row = row
    changed = False
    if "sourcesystem_cd" in row:
        new_row["sourcesystem_cd"] = source_id
        changed = True
    elif schema and schema == "i2b2metadata" and table and table == "table_access" and "c_table_cd" in row:
        ## Add to table_access c_table_cd
        ## HACK: Ensure c_table_cd always starts with i2b2_
        if not str(new_row["c_table_cd"]).startswith("i2b2_"):
            logger.warn("Adding prefix 'i2b2_' for c_table_cd column")
            new_row["c_table_cd"] = "{}{}".format("i2b2_", row["c_table_cd"])
        ## TODO: This is hard coding that the c_table_cd starts with i2b2_ - that may not always be the case!
        if "i2b2_" in new_row["c_table_cd"]:
            new_row["c_table_cd"] = new_row["c_table_cd"].replace("i2b2_", "i2b2_{}_".format(source_id))
            changed = True
        else:
            logger.warn("new_row[\"c_table_cd\"] ({}) in '{}.{}' does not contain 'i2b2_' so will not have the source_id/sourcesystem_cd ({}) added".format(
                new_row,
                schema,
                table,
                source_id
            ))
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
            cursor.execute(translator_deletes, ["i2b2_{}_%".format(source_id), *[source_id] * 3])
        logger.debug("DELETEd source_ids: {}\n{}".format(source_ids, cursor.query))
        return True
    except Exception as e:
        db_conn.rollback()
        logger.error("Failed to complete database DELETEs...\n{}".format(e))
        return False

def update_headers(row:dict, table_headers:list) -> tuple[dict, bool]:
    """Compare headers in use (possibly from CSV file) and table headers to ensure nothing critical is missing
    
    This can inject sourcesystem_cd to ensure the entries are trackable and cleanable by the api
    """
    changed = False
    new_row = row
    if "sourcesystem_cd" in table_headers and "sourcesystem_cd" not in row.keys():
        # logger.info("Adding 'sourcesystem_cd' to data...")
        new_row["sourcesystem_cd"] = None
        changed = True
    return new_row, changed

def push_csv_to_database(db_conn, source_id:str, prepared_file_paths:list, delim:str = ","):
    """Push any csv data which is listed to the database
    
    Sniffs for header line so should work with or without heading line - using database columns if no header
    Replace "current_timestamp" with actual datetime in memory before inserting
    Update source_id fields

    Also does some fixing of NULL and empty data

    :param prepared_file_paths: list of full filepaths
    :return: Boolean success/failure
    """
    ## TODO: Work with unknown delimiters (mostly , or ;)? Or always with ,?
    if prepared_file_paths and len(prepared_file_paths) > 0:
        upload_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.0")
        logger.info("Updating database with data from CSV files... {}".format(prepared_file_paths))
        ## Update col limits
        type_limits = app.config["i2b2db_col_limits"]
        if type_limits:
            for current_schema, current_tables in type_limits.items():
                if current_tables:
                    for current_table, table_limits in current_tables.items():
                        update_col_limits(db_conn, current_schema, current_table, table_limits)
                        # logger.debug("Col limits after updating: {}".format(_get_col_limits(db_conn, current_schema, current_table)))
        try:
            cursor = db_conn.cursor()
            for csv_filepath in prepared_file_paths:
                csv_filename = os.path.basename(csv_filepath)
                ## TOOD: More sanity checks, this is making dangerous assumptions about the file naming policy
                if source_id in csv_filename:
                    current_schema = csv_filename.split(".")[1]
                    current_table = csv_filename.split(".")[2]
                else:
                    current_schema = csv_filename.split(".")[0]
                    current_table = csv_filename.split(".")[1]
                logger.debug("Interpreting CSV file '{}' (schema: {}, table: {}) with delimiter '{}'...".format(
                    csv_filename,
                    current_schema,
                    current_table,
                    delim
                    ))
                ## Get dict of cols and length so we can update and trim our data to fit
                col_limits = _get_col_limits(db_conn, current_schema, current_table)
                with open(csv_filepath, 'r') as f:
                    ## Check for header line and reset to start of file
                    file_headers = None
                    has_header = False
                    reader = csv.reader(f, delimiter = delim)
                    first_line = next(reader)
                    table_headers = list(_get_col_limits(db_conn, current_schema, current_table).keys())
                    logger.debug("Got headers from database table... {}".format(table_headers))
                    logger.debug("Checking headers in file '{}' - first line: {}".format(csv_filepath, first_line))
                    non_header_test = ["", "null", "current_timestamp"]
                    if not any(True if col is None or col.isnumeric() or col.lower() in non_header_test else False for col in first_line):
                        has_header = True
                        logger.debug("Looks like a HEADER line")
                    if has_header:
                        file_headers = first_line
                    else:
                        ## Use headers from database columns (we can only hope they're the same and in order!)
                        ## TODO: Sanity check for number of columns
                        logger.debug("Using header from database table...")
                        file_headers = table_headers
                    logger.debug("file_headers: {}".format(file_headers))
                    ## This can be somewhere in-between the file headers and the table_headers!
                    insert_headers = list(update_headers({k:None for k in file_headers},table_headers)[0].keys())
                    logger.debug("insert_headers: {}".format(insert_headers))
                    query = 'insert into {schema}.{table}({headers}) values ({values}) ON CONFLICT DO NOTHING;'
                    query = query.format(
                        schema = current_schema,
                        table = current_table,
                        headers = ",".join(insert_headers),
                        values = ','.join(['%s'] * len(insert_headers))
                    )
                    logger.debug("prepared query: {}".format(query))
                    ## Reset to beginning of file in case first line is data, not header
                    f.seek(0)

                    reader = csv.DictReader(f, fieldnames = file_headers, delimiter = delim)
                    if has_header:
                        ## Skip header
                        next(reader)
                    for data in reader:
                        # logger.debug("data line: {}".format(data))
                        ## Add columns such as "sourcesystem_cd" if they are not in the CSV file
                        data, changed = update_headers(data, table_headers)
                        ## Ensure its an sql null not a string 'null' - int's don't like strings
                        ## Also replace "current_timestamp" with the actual current timestamp
                        data = {k:(None if v == '' or v == 'NULL' else upload_time if v == 'current_timestamp' else v) for k,v in data.items()}
                        if "c_dimcode" in data and data["c_dimcode"] is None:
                            ## TODO:(Be less hacky) Hacky fix for NULL and "NULL" being the same once the csv is loaded!
                            logger.debug("c_dimcode is None (changing to 'NULL'): {}".format(data["c_dimcode"]))
                            data["c_dimcode"] = "NULL"
                        if "import_date" in data:
                            ## In case it wasn't set as "current_timestamp" and therefore picked up already
                            data["import_date"] = upload_time
                        # logger.debug("CSV data: {}".format(data))
                        ## Add source_id where necessary
                        data, changed = add_source(data, source_id, current_schema, current_table)
                        ## Ensure data fits the database column length restrictions (for varchar)
                        data, changed = shorten_csv_data(data, col_limits, current_schema, current_table)
                        ## Insert line of data
                        cursor.execute(query, list(data.values()))
                ## NOTE: Changed is updated per function each line, so this will only see if the last function applied to the last line made a change
                logger.info("INSERTed values for '{}.{}' Added source: {}".format(current_schema, current_table, changed))
            return True
        except Exception as e:
            db_conn.rollback()
            logger.error("Failed to complete database INSERTs...\n{}".format(e))
            return False

def update_patient_count(db_conn) -> bool:
    """Run the patient count SQL against the i2b2 postgres database"""
    patientcount_update_resource_file = "patient_count.sql"
    logger.info("Updating patient counts in i2b2 by running file: {}".format(patientcount_update_resource_file))
    query_success = queries.run_sql_file(db_conn, patientcount_update_resource_file)
    # PGPASSWORD=$DB_ADMIN_PASS /usr/bin/psql -v ON_ERROR_STOP=1 -v statement_timeout=120000 -L "$TEMPDIR/postgres.log" -q --host=$I2B2DBHOST --username=$DB_ADMIN_USER --dbname=$I2B2DBNAME -f "/patient_count.sql" | tee -a "$LOGFILE"
    return query_success
