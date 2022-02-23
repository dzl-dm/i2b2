""" model module
Use SQLAlchemy to setup connections to the i2b2 postgres database
All model classes will be able to use this connection
"""
from flask_sqlalchemy import SQLAlchemy
from .. import app

db = SQLAlchemy(app)
