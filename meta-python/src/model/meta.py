""" meta.py
The object-table classes for i2b2metadata schema tables relevant for metadata loading from CoMetaR
"""
from . import db

class TableAccess(db.Model):
    """Data model for TableAccess"""

    __tablename__ = 'table_access'
    __table_args__ = {"schema": "i2b2metadata"}

    id = db.Column(
        db.Integer,
        primary_key=True
    )
    username = db.Column(
        db.String(64),
        index=False,
        unique=True,
        nullable=False
    )
    email = db.Column(
        db.String(80),
        index=True,
        unique=True,
        nullable=False
    )
    created = db.Column(
        db.DateTime,
        index=False,
        unique=False,
        nullable=False
    )

class I2b2(db.Model):
    """Data model for I2b2"""

    __tablename__ = 'i2b2'
    __table_args__ = {"schema": "i2b2metadata"}

    id = db.Column(
        db.Integer,
        primary_key=True
    )
