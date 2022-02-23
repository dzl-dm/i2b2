""" data.py
The object-table classes for i2b2demodata schema tables relevant for metadata loading from CoMetaR
"""
from . import db

class Concept(db.Model):
    """Data model for Concept"""

    __tablename__ = 'concept_dimension'
    __table_args__ = {"schema": "i2b2demodata"}

    concept_path = db.Column(
        db.String(700),
        primary_key=True,
        index=True,
        nullable=False
    )
    concept_cd = db.Column(
        db.String(50)
    )
    name_char = db.Column(
        db.String(2000)
    )
    concept_blob = db.Column(
        db.Text
    )
    update_date = db.Column(
        db.DateTime
    )
    download_date = db.Column(
        db.DateTime
    )
    import_date = db.Column(
        db.DateTime
    )
    sourcesystem_cd = db.Column(
        db.String(50)
    )
    upload_id = db.Column(
        db.Integer
    )

class Modifier(db.Model):
    """Data model for modifier"""

    __tablename__ = 'modifier_dimension'
    __table_args__ = {"schema": "i2b2demodata"}

    modifier_path = db.Column(
        db.String(700),
        primary_key=True,
        index=True,
        nullable=False
    )
    modifier_cd = db.Column(
        db.String(50)
    )
    name_char = db.Column(
        db.String(2000)
    )
    modifier_blob = db.Column(
        db.Text
    )
    update_date = db.Column(
        db.DateTime
    )
    download_date = db.Column(
        db.DateTime
    )
    import_date = db.Column(
        db.DateTime
    )
    sourcesystem_cd = db.Column(
        db.String(50)
    )
    upload_id = db.Column(
        db.Integer
    )
