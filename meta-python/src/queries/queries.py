""" queries.py
Collect all sparql queries in one place
Utilise the skeleton files in resources directory
"""
from flask import current_app as app

import logging
logger = logging.getLogger(__name__)

import json
import os
import re

## For caching the queries
sparql_skeletons:dict = None

def run_sql_file(db_conn, filename) -> bool:
    """Use the database connection and filename supplied to run the SQL in the resources dir"""
    ## TODO: Concat resources dir (from config) with filename
    # files_dir = os.getcwd()
    files_dir = "/src/resources"
    filepath = os.path.join(files_dir, filename)
    if not os.path.exists(filepath):
        logger.warn("Could not find file: {}".format(filepath))
        return False
    with open(filepath, 'r') as file:
        sql_statement = file.read()
    logger.debug("Running SQL against connection '{}':\n{}".format(db_conn, sql_statement))
    try:
        cursor = db_conn.cursor()
        cursor.execute(sql_statement)
        logger.debug("SQL Statement complete with no database errors!")
        return True
    except Exception as e:
        db_conn.rollback()
        logger.error("Failed to run SQL statement...\n{}".format(e))
        return False

def top_elements(connection) -> dict:
    """Query fuseki/sparql for the parent element names and types"""
    logger.debug("Fetching top-level elements")
    sparql_query = _get_skeleton("query_top_elements")

    fuseki_endpoint = connection["prepared_request"].url
    ## Simple request using session (for connection pooling/reuse)
    response = connection["session"].get(fuseki_endpoint, params={"query": sparql_query}, timeout=connection["timeout"])
    logger.debug("Response (session): {}".format(response.text))

    data = response.json()
    jsonString = json.dumps(data)

    elements:dict = {}
    for child in data["results"]["bindings"]:
        elements[child["element"]["value"]] = child["type"]["value"]
    logger.debug("Found top-level elements: {}".format(elements))
    logger.debug(jsonString)
    return elements

def getChildren(connection, node_name):
    """Get all child elements of the given element"""
    logger.debug("fetching node children for {}".format(node_name))
    sparql_query = _get_skeleton("query_child_elements").replace("TOPELEMENT", "<"+node_name+">")

    fuseki_endpoint = connection["prepared_request"].url
    response = connection["session"].get(fuseki_endpoint, params={"query": sparql_query}, timeout=connection["timeout"])

    data = response.json()
    jsonString = json.dumps(data)

    children:dict = {}
    for child in data["results"]["bindings"]:
        children[child["element"]["value"]] = child["type"]["value"]
    logger.debug("Found children for '{}': {}".format(node_name, children))
    logger.debug(jsonString)
    return children

def getAttributes(connection, node_uri):
    """Use single query to get all the useful attributes of the node"""
    logger.debug("fetching node attributes/properties for {}".format(node_uri))
    sparql_query = _get_skeleton("query_attributes").replace("<CONCEPT>", "<"+node_uri+">")

    fuseki_endpoint = connection["prepared_request"].url
    response = connection["session"].get(fuseki_endpoint, params={"query": sparql_query}, timeout=connection["timeout"])

    data = response.json()
    jsonString = json.dumps(data)

    try:
        element:dict = {}
        element["name"] = getName(node_uri)
        singles_from_query = ["prefLabel", "displayLabel", "display", "datatype", "description"]
        for attrib in singles_from_query:
            if attrib in data["results"]["bindings"][0] and data["results"]["bindings"][0][attrib]["value"] != "":
                element[attrib] = _clean_label(data["results"]["bindings"][0][attrib]["value"])
            else:
                logger.warn("No '{}' available for node: {}".format(attrib, node_uri))
                element[attrib] = None

        lists_from_query = ["notations", "units"]
        ## NOTE: List delimiter in query is hard-coded as semi-colon ; TODO: Use a config and .replace()
        for attrib in lists_from_query:
            if attrib in data["results"]["bindings"][0]:
                ## TODO: Adjust query to get the tag and add that as the value
                element[attrib] = {_clean_label(k):None for k in data["results"]["bindings"][0][attrib]["value"].strip("[]").split(";")}
                # element[attrib] = _clean_label(data["results"]["bindings"][0][attrib]["value"])
            else:
                logger.warn("No '{}' available for node: {}".format(attrib, node_uri))
                element[attrib] = None

    except Exception as e:
        logger.error("Error processing attributes from query for concept: {}!\n{}".format(node_uri, e))
    finally:
        logger.debug(jsonString)
        logger.debug("Node data: {}".format(element))
    return element

def _clean_label(label:str) -> str:
    """Clean charachters we don't want in the database"""
    if label is None:
        return None
    _RE_COMBINE_WHITESPACE = re.compile(r"\s+")

    ## The database connection library (psycopg) takes care of most things!
    # label = label.replace("'", "''")
    ## TODO: Is it cleaner to let the library handle quotations? (but wait until we have the same output as old system)
    label = label.replace("\"", "&quot;")
    ## TODO: Existing export has trailing spaces, so don't strip them yet!
    # label = _RE_COMBINE_WHITESPACE.sub(" ", label).strip()
    label = _RE_COMBINE_WHITESPACE.sub(" ", label)
    return label

def getName(element_uri:str, include_prefix:bool = True) -> str:
    """Get the name portion of the element - replace prefix url with shorthand where possible"""
    ## TODO: Automate getting @prefix definitions from fuseki
    name = element_uri
    for k, v in app.config["generator_mappings"].items():
        name = name.replace(k, v)
    if not include_prefix:
        name = name.split(":")[1]
    logger.debug("Name of element ({}): {}".format(element_uri, name))
    return name

def _get_skeleton(query_name:str) -> str:
    """Read the file based on query_name and return its contents - cache in global dict"""
    global sparql_skeletons
    # logger.debug("Fetching skeleton query for: {}".format(query_name))
    if sparql_skeletons and query_name in sparql_skeletons:
        # logger.info("Found cached skeleton: {}".format(sparql_skeletons[query_name]))
        return sparql_skeletons[query_name]
    ## TODO: Define in config!
    query_file = "/src/resources/{}.txt".format(query_name)
    sparql_skeleton = ""
    if not os.path.exists(query_file):
        logger.warn("Could not find skeleton query for: {} - using filename: [{}] -> {}".format(query_name, os.getcwd(), query_file))
        return sparql_skeleton
    with open(query_file, 'r') as file:
        sparql_skeleton = file.read()
    if not sparql_skeletons:
        sparql_skeletons = {}
    sparql_skeletons[query_name] = sparql_skeleton
    logger.info("Found file-based skeleton ({}):\n{}".format(query_file, sparql_skeleton))
    return sparql_skeleton
