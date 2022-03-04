""" meta.py
The section for meta-data processing. Eg from CoMetaR
"""

from flask import current_app as app

import logging
logger = logging.getLogger(__name__)

# from flask import current_app as app

from datetime import datetime as dt
import model
import os
import psycopg2
import requests
import time
import yaml
from sqlalchemy import sql as sqlalch


app_settings:dict = {}
with open(os.getenv("APP_CONF_PATH"), "r") as yaml_file:
    app_settings = yaml.safe_load(yaml_file)

# postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(1, 20, user=os.getenv("DB_ADMIN_USER"),
#                                                          password=os.getenv("DB_ADMIN_PASS"),
#                                                          host=os.getenv("I2B2DBHOST"),
#                                                          port=os.getenv("I2B2DBPORT"),
#                                                          database=os.getenv("I2B2DBNAME"))

def update_meta_from_cometar():
    """Pull data from fuseki component of CoMetaR, then convert to SQL and update i2b2 database"""
    # pull_fuseki_data()
    # ttl_to_sql()
    # sql_to_i2b2()
    # insert_test_demodata()
    insert_sql_test()
    return True

def prepare_filesystem():
    """Ensure directories exist and old files are cleaned up before processing starts"""
    pass

def pull_fuseki_data():
    """Pull the data from fuseki and write as a local ttl file"""
    app.logger.debug("fetching data")
    import json
    import SPARQLWrapper
    global app_settings

    fuseki_query:str = ""
    with open("/src/resources/query_top_elements.txt", "r") as file:
        for line in file:
            fuseki_query += line

    app.logger.debug("fuseki_query: {}".format(fuseki_query))

    sparql = SPARQLWrapper.SPARQLWrapper(app_settings["fuseki_url"])
    sparql.setQuery(fuseki_query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)

    data = sparql.query().convert()
    jsonString = json.dumps(data)
    jsonFile = open("/top_level.json", "w")
    jsonFile.write(jsonString)
    jsonFile.close()
    app.logger.debug("Top level CoMetaR data ({}): {}".format("json", data))
    return True

def pull_fuseki_data2():
    """Pull the data from fuseki and write as a local ttl file"""
    app.logger.debug("fetching fuseki data")
    import json
    # import xml.etree.ElementTree as ET
    from xml.dom.minidom import parseString
    import SPARQLWrapper
    global app_settings

    fuseki_query:str = ""
    with open("/src/resources/query_top_elements.txt", "r") as file:
        file_text = ""
        for line in file:
            file_text += line
    fuseki_query = file_text
    app.logger.debug("fuseki_query: {}".format(fuseki_query))

    sparql = SPARQLWrapper.SPARQLWrapper(app_settings["fuseki_url"])
    sparql.setQuery(fuseki_query)

    ## XML Response
    sparql.setReturnFormat(SPARQLWrapper.XML)
    data = sparql.query().convert()
    app.logger.debug("Top level CoMetaR data ({}): {}".format("xml", data))
    # xmlTree = ET.ElementTree.parse(data)
    xmlDom = data
    xmlString = xmlDom.toxml()
    app.logger.debug("Top level CoMetaR data ({}): {}".format("xml tree", xmlString))
    # app.logger.debug("Top level CoMetaR data ({}): {}".format("xml tree", ET.dump(xmlTree)))
    xmlFile = open("/top_level.xml", "w")
    xmlFile.write(xmlString)

    ## JSON Response
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    data = sparql.query().convert()
    jsonString = json.dumps(data)
    jsonFile = open("/top_level.json", "w")
    jsonFile.write(jsonString)
    jsonFile.close()
    app.logger.debug("Top level CoMetaR data ({}):\n{}".format("json", data))
    result = data

    ## TSV Response
    sparql.setReturnFormat(SPARQLWrapper.TSV)
    data = sparql.query().convert()
    tsvString = data.decode()
    app.logger.debug("Top level CoMetaR data ({}):\n{}".format("tsv", tsvString))
    tsvFile = open("/top_level.tsv", "w")
    tsvFile.write(tsvString)
    tsvFile.close()

    # app.logger.debug("Top level CoMetaR data ({}): {}".format(data.status_code, data.content))
    # with open("/top_level.ttl", "w") as file:
    #     file.write(data.content)
    return result

def pull_fuseki_data3():
    """Pull the child data from fuseki and write as a local ttl file"""
    app.logger.debug("fetching child data")
    import json
    import SPARQLWrapper
    global app_settings

    fuseki_query:str = ""
    with open("/src/resources/query_child_elements.txt", "r") as file:
        for line in file:
            fuseki_query += line
    # fuseki_query = fuseki_query.replace("TOPELEMENT", "<http://data.dzl.de/ont/dwh#HealthQuest>")
    # fuseki_query = fuseki_query.replace("TOPELEMENT", "<http://purl.bioontology.org/ontology/SNOMEDCT/418715001>")
    fuseki_query = fuseki_query.replace("TOPELEMENT", "<http://purl.bioontology.org/ontology/SNOMEDCT/365981007>")

    app.logger.debug("fuseki_query: {}".format(fuseki_query))

    sparql = SPARQLWrapper.SPARQLWrapper(app_settings["fuseki_url"])
    sparql.setQuery(fuseki_query)
    sparql.setReturnFormat(SPARQLWrapper.JSON)

    data = sparql.query().convert()
    jsonString = json.dumps(data)
    jsonFile = open("/HealthQuest.json", "w")
    jsonFile.write(jsonString)
    jsonFile.close()
    app.logger.debug("HealthQuest CoMetaR data ({}): {}".format("json", data))
    return data

def sql_to_i2b2():
    """Run the SQL against the i2b2 postgres database to insert the rules"""
    ## meta

    ## data
    pass

def update_patient_count():
    """Run the patient count SQL against the i2b2 postgres database"""
    # PGPASSWORD=$DB_ADMIN_PASS /usr/bin/psql -v ON_ERROR_STOP=1 -v statement_timeout=120000 -L "$TEMPDIR/postgres.log" -q --host=$I2B2DBHOST --username=$DB_ADMIN_USER --dbname=$I2B2DBNAME -f "/patient_count.sql" | tee -a "$LOGFILE"

    pass

def insert_test_demodata():
    """Check I'm talking to the database correctly"""
    new_demodata = model.Concept("test\path", "test code", "Test code name", dt.now(), dt.now(), dt.now(), "TEST", None)
    model.db.session.add(new_demodata)  # Adds new User record to database
    model.db.session.commit()
    pass

def insert_sql_test():
    """Test running sql file against database"""
    logger.info("Attempting to run an sql file via sqlalchemy")
    # test_file = "/var/tmp/meta/i2b2-sql/meta.sql"
    test_file = "/var/tmp/meta/i2b2-sql/data.sql"
    with open(test_file, 'r') as file:
        sql_data = file.read()
    logger.debug("Running sql against db: {}".format(sql_data))

    ## Basic execution:
    result = model.db.engine.execute(sqlalch.text(sql_data).execution_options(autocommit=True))

    ## OR maybe I need to use it like this... api > 1.4
    # with model.db.engine.connect() as connection:
    #     result = connection.execute(sqlalch.text(sql_data))    

    logger.info("Result from running sql file ({}): {}".format(test_file, result))
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

