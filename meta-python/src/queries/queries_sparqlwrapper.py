""" queries.py
Collect all sparql queries in one place
Utilise the skeletons in resources
"""
from flask import current_app as app

import logging
app.logger = logging.getLogger(__name__)

from io import StringIO
import json
import os
import re
import requests
# import pycurl
import SPARQLWrapper
import sys

sparql_skeletons:dict = None

def get_connection(fuseki_endpoint:str, connection_type:str = "requests"):
    """Run the respective function to get the connection of the type requested"""
    connections = {"requests": "_get_requests_connection", "sqparql_wrapper": "_get_sparql_wrapper"}
    connection_function = connections.get(connection_type, "requests")
    connection = None
    try:
        app.logger.debug("Getting connection from '{}.{}'".format(sys.modules[__name__], connection_function))
        connection = getattr(sys.modules[__name__], connection_function)(fuseki_endpoint)
    except Exception as e:
        app.logger.error("Could not call the function to get the real connection!")
        app.logger.error("Requested type: {}\nSelected function: {}\nError:\n{}".format(connection_type, connection_function, e))
    return connection

def _get_sparql_wrapper(fuseki_endpoint:str):
    """Use the endpoint-url to setup the wrapper, which is needed to make quieries"""
    sparql = SPARQLWrapper.SPARQLWrapper(fuseki_endpoint)
    sparql.setUseKeepAlive()
    app.logger.debug("Using KeepAlive with SPARQL Wrapper connections")
    ## See issue: https://github.com/RDFLib/sparqlwrapper/issues/2
    return sparql

def _get_requests_connection(fuseki_endpoint:str) -> dict:
    """Get session and prepared request, also supply a default timeout"""
    connection = {"session": _get_request_session(), "prepared_request": _get_prepped_request(fuseki_endpoint), "timeout": (3, 60)}
    return connection

def _get_request_session():
    """Create a session to utilise connection pooling - possibly more later"""
    new_session = requests.Session()
    return new_session

def _get_prepped_request(fuseki_endpoint:str):
    """Create a prepared request for the fuseki endpoint"""
    ## timeout is not accepted, limiting the benefit of preparing the request... :/
    req = requests.Request("GET", fuseki_endpoint)
    prep_req = req.prepare()
    return prep_req

def top_elements(prepped_request, req_sess) -> dict:
    """Query fuseki/sparql for the parent element names and types"""
    app.logger.debug("Fetching top-level elements")
    sparql_query = _get_skeleton("query_top_elements")

    ## USE PycURL
    # curl = pycurl.Curl()
    ## Try GET
    # url="http://localhost:7200/sparql?name=&infer=true&sameAs=false&query=select+*+where+%7B+%0A%09%3Fs+%3Fp+%3Fo+.%0A%7D+limit+100+%0A&execute="
    # query_url="{fuseki_endpoint}?query=\"{sparql_query}\"".format(fuseki_endpoint=fuseki_endpoint, sparql_query=urllib.parse.urlencode(sparql_query))
    # app.logger.debug("Will try to get response from: {}".format(query_url))
    # curl.setopt(curl.URL, query_url)
    # curl.setopt(curl.USERPWD, '%s:%s' % (' ' , ' '))

    ## Try POST
    # curl.setopt(curl.URL, fuseki_endpoint)
    # query_data = {"query": sparql_query}
    # encoded_query_data = urllib.parse.urlencode(query_data)
    # curl.setopt(curl.POSTFIELDS, encoded_query_data)

    # response_buffer = StringIO()
    # curl.setopt(curl.WRITEFUNCTION, response_buffer.write)
    # curl.perform()
    # curl.close()
    # response_value = response_buffer.getvalue()
    # app.logger.debug("Response: {}".format(response_value))
    # data = response_value

    ## USE requests
    # query_url="{fuseki_endpoint}?query=\"{sparql_query}\"".format(fuseki_endpoint=fuseki_endpoint, sparql_query=urllib.parse.urlencode(sparql_query))
    # query_url="{fuseki_endpoint}?query=\"{sparql_query}\"".format(fuseki_endpoint=fuseki_endpoint, sparql_query=sparql_query.replace("\n", " "))
    # app.logger.debug("Will try to get response from: {}".format(query_url))
    # req = requests.get(query_url)

    fuseki_endpoint = prepped_request.url
    ## Simple, standalone request
    # app.logger.debug("Will try to get response from: {}".format(prepped_request.url))
    # response = requests.get(fuseki_endpoint, params={"query": sparql_query}, timeout=(3, 60))
    # app.logger.debug("Response (single req): {}".format(response.text))

    ## Simple request using session (for connection pooling/reuse)
    response = req_sess.get(fuseki_endpoint, params={"query": sparql_query}, timeout=(3, 60))
    app.logger.debug("Response (session): {}".format(response.text))

    ## TODO-WIP: Prepared request using session (for connection pooling/reuse)
    # prepped_request.params={"query": sparql_query}
    # ## Use the session to send the request (and reuse connections)
    # response = req_sess.send(prepped_request, timeout=(3, 60))
    # app.logger.debug("Will try to get response from: {}".format(prepped_request.url))
    # app.logger.debug("Response (prepared req, session): {}".format(response.text))

    data = response.json()
    jsonString = json.dumps(data)

    elements:dict = {}
    for child in data["results"]["bindings"]:
        elements[child["element"]["value"]] = child["type"]["value"]
    app.logger.debug("Found top-level elements: {}".format(elements))
    app.logger.debug(jsonString)
    return elements

def getChildren(prepped_request, req_sess, node_name):
    """Get all child elements of the given element"""
    app.logger.debug("fetching node children for {}".format(node_name))
    sparql_query = _get_skeleton("query_child_elements").replace("TOPELEMENT", "<"+node_name+">")

    fuseki_endpoint = prepped_request.url
    response = req_sess.get(fuseki_endpoint, params={"query": sparql_query}, timeout=(3, 60))

    data = response.json()
    jsonString = json.dumps(data)

    children:dict = {}
    for child in data["results"]["bindings"]:
        children[child["element"]["value"]] = child["type"]["value"]
    app.logger.debug("Found children for '{}': {}".format(node_name, children))
    app.logger.debug(jsonString)
    return children

def getAttributes(prepped_request, req_sess, node_uri):
    """Use single query to get all the useful attributes of the node"""
    app.logger.debug("fetching node attributes/properties for {}".format(node_uri))
    sparql_query = _get_skeleton("query_attributes").replace("<CONCEPT>", "<"+node_uri+">")

    fuseki_endpoint = prepped_request.url
    response = req_sess.get(fuseki_endpoint, params={"query": sparql_query}, timeout=(3, 60))

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
                app.logger.warn("No '{}' available for node: {}".format(attrib, node_uri))
                element[attrib] = None

        lists_from_query = ["notations", "units"]
        for attrib in lists_from_query:
            if attrib in data["results"]["bindings"][0]:
                ## TODO: Adjust query to get the tag and add that as the value
                element[attrib] = {_clean_label(k):None for k in data["results"]["bindings"][0][attrib]["value"].strip("[]").split(",")}
                # element[attrib] = _clean_label(data["results"]["bindings"][0][attrib]["value"])
            else:
                app.logger.warn("No '{}' available for node: {}".format(attrib, node_uri))
                element[attrib] = None

    except Exception as e:
        app.logger.error("Error processing attributes from query for concept: {}!\n{}".format(node_uri, e))
    finally:
        app.logger.debug(jsonString)
        app.logger.debug("Node data: {}".format(element))
    return element

def top_elements_sparqlwrap(sparql_wrapper) -> dict:
    """Query fuseki/sparql for the parent element names and types"""
    app.logger.debug("Fetching top-level elements")
    sparql_query = _get_skeleton("query_top_elements")
    sparql_wrapper.setQuery(sparql_query)
    sparql_wrapper.setReturnFormat(SPARQLWrapper.JSON)
    data = sparql_wrapper.query().convert()
    app.logger.debug("Response (raw): {}".format(data))
    jsonString = json.dumps(data)
    app.logger.debug("Response (stringified JSON): {}".format(jsonString))

    elements:dict = {}
    for child in data["results"]["bindings"]:
        elements[child["element"]["value"]] = child["type"]["value"]
    app.logger.debug("Found top-level elements: {}".format(elements))
    app.logger.debug(jsonString)
    return elements

def getChildren_sparqlwrap(sparql_wrapper, node_name):
    """Get all child elements of the given element"""
    app.logger.debug("fetching node children for {}".format(node_name))
    sparql_query = _get_skeleton("query_child_elements").replace("TOPELEMENT", "<"+node_name+">")
    sparql_wrapper.setQuery(sparql_query)
    sparql_wrapper.setReturnFormat(SPARQLWrapper.JSON)
    data = sparql_wrapper.query().convert()
    jsonString = json.dumps(data)

    children:dict = {}
    for child in data["results"]["bindings"]:
        children[child["element"]["value"]] = child["type"]["value"]
    app.logger.debug("Found children for '{}': {}".format(node_name, children))
    app.logger.debug(jsonString)
    return children

def getAttributes_sparqlwrap(sparql_wrapper, node_uri):
    """Use single query to get all the useful attributes of the node"""
    app.logger.debug("fetching node attributes/properties for {}".format(node_uri))
    sparql_query = _get_skeleton("query_attributes").replace("<CONCEPT>", "<"+node_uri+">")
    sparql_wrapper.setQuery(sparql_query)
    sparql_wrapper.setReturnFormat(SPARQLWrapper.JSON)
    # app.logger.debug("Running query: {}".format(sparql_query))
    data = sparql_wrapper.query().convert()
    jsonString = json.dumps(data)

    try:
        element:dict = {}
        element["name"] = getName(node_uri)
        singles_from_query = ["prefLabel", "displayLabel", "display", "datatype", "description"]
        for attrib in singles_from_query:
            if attrib in data["results"]["bindings"][0] and data["results"]["bindings"][0][attrib]["value"] != "":
                element[attrib] = _clean_label(data["results"]["bindings"][0][attrib]["value"])
            else:
                app.logger.warn("No '{}' available for node: {}".format(attrib, node_uri))
                element[attrib] = None

        lists_from_query = ["notations", "units"]
        for attrib in lists_from_query:
            if attrib in data["results"]["bindings"][0]:
                ## TODO: Adjust query to get the tag and add that as the value
                element[attrib] = {_clean_label(k):None for k in data["results"]["bindings"][0][attrib]["value"].strip("[]").split(",")}
                # element[attrib] = _clean_label(data["results"]["bindings"][0][attrib]["value"])
            else:
                app.logger.warn("No '{}' available for node: {}".format(attrib, node_uri))
                element[attrib] = None

    except Exception as e:
        app.logger.error("Error processing attributes from query for concept: {}!\n{}".format(node_uri, e))
    finally:
        app.logger.debug(jsonString)
        app.logger.debug("Node data: {}".format(element))
    return element

def getLabel(node_name):
    """Get the label of the given element"""
    global sparql
    app.logger.debug("fetching node label property for {}".format(node_name))
    sparql_query = _get_skeleton("query_label").replace("<CONCEPT>", "<"+node_name+">")
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    # app.logger.debug("Running query: {}".format(sparql_query))
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

def getUnits(node_name):
    """Get the list of units and their tags for the given element"""
    app.logger.debug("fetching node units for {}".format(node_name))
    sparql_query = _get_skeleton("query_unit").replace("<CONCEPT>", "<"+node_name+">")
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    data = sparql.query().convert()
    jsonString = json.dumps(data)
    app.logger.debug(jsonString)

    try:
        units:dict = {}
        for unit in data["results"]["bindings"]:
            units[_clean_label(unit["unit"]["value"])] = _clean_label(unit["unit"]["type"])
        app.logger.debug("Found raw units for concept: {}".format(units))
    except Exception as e:
        units = ""
        app.logger.error("No unit for concept: {}!\n{}".format(node_name, e))
    return units

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
    ## TODO: Automate getting @prefix definitions from fuseki
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
    # app.logger.debug("Fetching skeleton query for: {}".format(query_name))
    if sparql_skeletons and query_name in sparql_skeletons:
        # app.logger.info("Found cached skeleton: {}".format(sparql_skeletons[query_name]))
        return sparql_skeletons[query_name]
    ## TODO: Define in config!
    query_file = "/src/resources/{}.txt".format(query_name)
    sparql_skeleton = ""
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
