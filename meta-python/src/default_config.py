""" default_config.py
Config class to setup defaults
"""
import os

basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    """Set Flask configuration from .env file."""

    ## General Config
    # SECRET_KEY = os.environ.get('SECRET_KEY')
    SECRET_KEY = os.urandom(12)
    FLASK_APP = os.environ.get('FLASK_APP')
    FLASK_ENV = os.environ.get('FLASK_ENV')

    ## Database
    ## TODO: if exists, use full url variable
    # SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")
    SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(os.environ.get("DB_ADMIN_USER"), os.environ.get("DB_ADMIN_PASS"), os.environ.get("I2B2DBHOST"), os.environ.get("I2B2DBPORT"), os.environ.get("I2B2DBNAME"))
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
