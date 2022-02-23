""" db_connection.py
Use SQLAlchemy and psycopg2 to manage connections to the i2b2 postgres database
"""
from flask_sqlalchemy import SQLAlchemy


engine = create_engine('postgresql+psycopg2://scott:tiger@localhost/mydatabase')


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)

    def __repr__(self):
        return '<User %r>' % self.username



from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Model_name(db.Model):
    __tablename__ = 'table_name'

    field1_name = db.Column(db.Field1Type, primary_key...)
    field2_name = db.Column(db.Field2Type)
    field3_name = db.Column(db.Field3Type)

    def __init__(self, Field1_name,Field1_name,Field1_name):
        self.field1_name = field1_name
        self.field2_name = field2_name
        self.field3_name = field3_name

    def __repr__(self):
        return f"<statement>"
