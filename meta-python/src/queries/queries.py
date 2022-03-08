""" queries.py
Collect all sparql queries in one place
Utilise the skeletons in resources
"""
from flask import current_app as app

import json
import os
import re
import SPARQLWrapper

sparql = SPARQLWrapper.SPARQLWrapper(app.config["fuseki_url"])
sparql_skeletons:dict = None

def getTopElements():
    """Get all top level nodes"""
    app.logger.debug("Fetching top-level elements")
    sparql_query = _get_skeleton("query_top_elements")
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    data = sparql.query().convert()
    jsonString = json.dumps(data)

    elements:dict = {}
    for child in data["results"]["bindings"]:
        elements[child["element"]["value"]] = child["type"]["value"]
    app.logger.debug("Found top-level elements: {}".format(elements))
    app.logger.debug(jsonString)
    return elements

def getChildren(node_name):
    """Get all child elements of the given element"""
    app.logger.debug("fetching node label property for {}".format(node_name))
    sparql_query = _get_skeleton("query_child_elements").replace("TOPELEMENT", "<"+node_name+">")
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    data = sparql.query().convert()
    jsonString = json.dumps(data)

    children:dict = {}
    for child in data["results"]["bindings"]:
        children[child["element"]["value"]] = child["type"]["value"]
    app.logger.debug("Found children for '{}': {}".format(node_name, children))
    app.logger.debug(jsonString)
    return children

def getParent(node_name):
    """Get parent when skos:broader exists"""
    pass

def getType(node_name):
    """Get rdf type (skos:concept etc)"""
    pass

def getLabel(node_name):
    """Get the label of the given element"""
    global sparql
    app.logger.debug("fetching node label property for {}".format(node_name))
    sparql_query = _get_skeleton("query_label").replace("<CONCEPT>", "<"+node_name+">")
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    app.logger.debug("Running query: {}".format(sparql_query))
    data = sparql.query().convert()
    jsonString = json.dumps(data)

    try:
        label = data["results"]["bindings"][0]["label"]["value"]
        app.logger.debug("Found label for '{}': {}".format(node_name, label))
    except Exception as e:
        app.logger.error("No prefLabel for concept: {}!\n{}".format(node_name, e))
        label = "ERROR"
    finally:
        app.logger.debug(jsonString)
    return _clean_label(label)

def getDisplayLabel(node_name):
    """Get the display-label of the given element"""
    app.logger.debug("fetching node display-label property for {}".format(node_name))
    sparql_query = _get_skeleton("query_display_label").replace("<CONCEPT>", "<"+node_name+">")
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    data = sparql.query().convert()
    jsonString = json.dumps(data)
    app.logger.debug(jsonString)

    try:
        display_label = data["results"]["bindings"][0]["displayLabel"]["value"]
        app.logger.debug("Found display-label for '{}': {}".format(node_name, display_label))
    except Exception as e:
        app.logger.error("No display-label for concept: {}!\n{}".format(node_name, e))
        display_label = None
    finally:
        app.logger.debug(jsonString)
    return _clean_label(display_label)

def getDatatypeXml(node_name, fetch_time):
    """Get the xml datatype of the given element"""
    app.logger.debug("fetching node datatype property for {}".format(node_name))
    sparql_query = _get_skeleton("query_datatype").replace("<CONCEPT>", "<"+node_name+">")
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    data = sparql.query().convert()
    jsonString = json.dumps(data)
    app.logger.debug(jsonString)

    types = {"Integer": ["int", "integer"], "Float": ["float", "dec", "decimal"], "largeString": ["largestring"], "String": ["string", "str"]}
    incoming_types = {v:k for k, l in types.items() for v in l}
    app.logger.debug("incoming types reverse map: {}".format(incoming_types))

    datatype_xml = "NULL"
    try:
        incoming_type = data["results"]["bindings"][0]["datatype"]["value"]
        if incoming_type != "":
            datatype = incoming_types.get(incoming_type, "String")
            datatype_xml = "'<ValueMetadata><Version>3.02</Version><CreationDateTime>{fetch_time}</CreationDateTime><DataType>{datatype}</DataType><Oktousevalues>Y</Oktousevalues></ValueMetadata>'".format(fetch_time=fetch_time, datatype=datatype)
        app.logger.debug("Found datatype for '{}': {}".format(node_name, datatype))
    except Exception as e:
        app.logger.error("No datatype for concept: {}!\n{}".format(node_name, e))
    return _clean_label(datatype_xml)

def getDatatypeRaw(node_name):
    """Get the xml datatype of the given element"""
    app.logger.debug("fetching node datatype property for {}".format(node_name))
    sparql_query = _get_skeleton("query_datatype").replace("<CONCEPT>", "<"+node_name+">")
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    data = sparql.query().convert()
    jsonString = json.dumps(data)
    app.logger.debug(jsonString)

    try:
        incoming_type = data["results"]["bindings"][0]["datatype"]["value"]
        app.logger.debug("Found raw datatype for concept: {}".format(incoming_type))
    except Exception as e:
        incoming_type = ""
        app.logger.error("No datatype for concept: {}!\n{}".format(node_name, e))
    return _clean_label(incoming_type)

def getDescription(node_name):
    """Get the description of the given element"""
    app.logger.debug("fetching node display property for {}".format(node_name))
    sparql_query = _get_skeleton("query_description").replace("<CONCEPT>", "<"+node_name+">")
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    data = sparql.query().convert()
    jsonString = json.dumps(data)

    description = data["results"]["bindings"][0]["description"]["value"]
    app.logger.debug("Found display value for '{}': {}".format(node_name, description))
    app.logger.debug(jsonString)
    return _clean_label(description)

def getDisplay(node_name):
    """Get the display-mode information of the given element"""
    app.logger.debug("fetching node display property for {}".format(node_name))
    sparql_query = _get_skeleton("query_display").replace("<CONCEPT>", "<"+node_name+">")

    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    data = sparql.query().convert()
    jsonString = json.dumps(data)

    try:
        display = data["results"]["bindings"][0]["display"]["value"]
        app.logger.debug("Found display value for '{}': {}".format(node_name, display))
    except Exception as e:
        app.logger.error("No display for concept: {}!\n{}".format(node_name, e))
        display = None
    finally:
        app.logger.debug(jsonString)
    return _clean_label(display)

def getNotations(node_name):
    """Get the notations of the given element"""
    app.logger.debug("fetching node notation propert(-y,-ies) for {}".format(node_name))
    sparql_query = _get_skeleton("query_notations").replace("<CONCEPT>", "<"+node_name+">")

    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)

    data = sparql.query().convert()
    jsonString = json.dumps(data)
    notations = []
    for notation in data["results"]["bindings"]:
        notations.append(_clean_label(notation["notation"]["value"]))
    notations = {}
    for notation in data["results"]["bindings"]:
        try:
            lang = _clean_label(notation["notation"]["xml:lang"])
        except Exception as e:
            lang = ""
        finally:
            notations[_clean_label(notation["notation"]["value"])] = lang
    app.logger.debug("Found notation value for '{}': {}".format(node_name, notations))
    app.logger.debug(jsonString)
    return notations

def _clean_label(label:str) -> str:
    """Clean charachters we don't want in the database"""
    if label is None:
        return None
    _RE_COMBINE_WHITESPACE = re.compile(r"\s+")

    label = label.replace("'", "''")
    label = label.replace("\"", "&quot;")
    # label = label.replace("\\s+", " ")
    label = _RE_COMBINE_WHITESPACE.sub(" ", label).strip()
    return label

def getName(element_uri:str, include_prefix:bool = True) -> str:
    """Get the name portion of the element - replace prefix url with shorthand where possible"""
    name = element_uri
    for k, v in app.config["generator_mappings"].items():
        name = name.replace(k, v)
    if not include_prefix:
        name = name.split(":")[1]
    app.logger.debug("Name of element ({}): {}".format(element_uri, name))
    return name

def _get_skeleton(query_name:str) -> str:
    """Read the file based on query_name and return its contents - cache in global dict"""
    global sparql_skeletons
    app.logger.debug("Fetching skeleton query for: {}".format(query_name))
    sparql_skeleton = ""
    if sparql_skeletons and query_name in sparql_skeletons:
        app.logger.info("Found cached skeleton: {}".format(sparql_skeleton))
        return sparql_skeletons[query_name]
    query_file = "/src/resources/{}.txt".format(query_name)
    if not os.path.exists(query_file):
        app.logger.warn("Could not find skeleton query for: {} - using filename: [{}] -> {}".format(query_name, os.getcwd(), query_file))
        return sparql_skeleton
    with open(query_file, 'r') as file:
        sparql_skeleton = file.read()
    if not sparql_skeletons:
        sparql_skeletons = {}
    sparql_skeletons[query_name] = sparql_skeleton
    app.logger.info("Found file-based skeleton ({}):\n{}".format(query_file, sparql_skeleton))
    return sparql_skeleton
