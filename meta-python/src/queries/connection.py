""" connection.py
Functions to manage the connection to fuseki
"""
from flask import current_app as app

import logging
logger = logging.getLogger(__name__)

import psycopg2
import psycopg2.sql
import requests
import SPARQLWrapper
import sys

def get_fuseki_connection(fuseki_endpoint:str, connection_type:str = "requests", source_id:str = "UNKNOWN"):
    """Run the respective function to get the connection of the type requested"""
    connections = {"requests": "_get_requests_connection", "sqparql_wrapper": "_get_sparql_wrapper"}
    connection_function = connections.get(connection_type, "requests")
    connection = None
    try:
        logger.debug("Getting connection from '{}.{}'".format(sys.modules[__name__], connection_function))
        connection = getattr(sys.modules[__name__], connection_function)(fuseki_endpoint)
        connection["source_id"] = source_id
    except Exception as e:
        logger.error("Could not call the function to get the real connection!")
        logger.error("Requested type: {}\nSelected function: {}\nError:\n{}".format(connection_type, connection_function, e))
    return connection

def _get_sparql_wrapper(fuseki_endpoint:str):
    """Use the endpoint-url to setup the wrapper, which is needed to make quieries"""
    sparql = SPARQLWrapper.SPARQLWrapper(fuseki_endpoint)
    sparql.setUseKeepAlive()
    logger.debug("Using KeepAlive with SPARQL Wrapper connections")
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

def get_database_connection(database_host:str, database_name:str, database_user:str, database_password):
    """Get a connection to the database which we can reuse elsewhere"""
    logger.debug("Connection to db...")
    try:
        conn = psycopg2.connect (
            host = database_host,
            dbname = database_name,
            user = database_user,
            password = database_password
        )
        logger.info("Connection to postgres successful! {}".format(conn))
    except:
        logger.warn("Connection to postgres UN-successful! {}".format(conn))
        conn = None
    return conn
