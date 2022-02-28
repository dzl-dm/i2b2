""" model module
Use SQLAlchemy to setup connections to the i2b2 postgres database
All model classes will be able to use this connection
"""
import logging
logger = logging.getLogger(__name__)

from flask_sqlalchemy import SQLAlchemy
from flask import current_app as app

logger.debug("SQLALCHEMY_DATABASE_URI: {}".format(app.config['SQLALCHEMY_DATABASE_URI']))
logger.debug("FLASK_APP: {}".format(app.config['FLASK_APP']))
logger.debug("FLASK_ENV: {}".format(app.config['FLASK_ENV']))

db = SQLAlchemy(app)
