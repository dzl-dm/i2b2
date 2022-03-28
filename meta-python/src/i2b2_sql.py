""" i2b2_sql.py
Generate SQL statements to insert elements into i2b2 database
"""
from flask import current_app as app

import logging
app.logger = logging.getLogger(__name__)

def write_sql_for_node(c_hlevel, notations, concept_long, label, datatypexml, description, isModifier, appliedPath, current_timestamp, visualAttribute, isRootElement, displayLabel, children):
    """Write statements for concept. In case of not exactly one concept notation, String notation will be "NULL"."""

    _meta_i2b2(c_hlevel, notations, concept_long, label, description, visualAttribute, current_timestamp, isModifier, appliedPath, datatypexml)

    if isRootElement:
        _meta_table_access(concept_long, displayLabel, visualAttribute)
    if notations is None:
        if isModifier:
            _demo_modifier(notations, concept_long, label, current_timestamp)
        else:
            _demo_concept(notations, concept_long, label, current_timestamp)

    ## in case of multiple notations
    if len(notations) > 1:
        ## If the concept also has children, insert an additional i2b2 path layer.
        if len(children) > 0:
            c_hlevel += 1
            ## H für HIDDEN
            visualAttribute = "MH"
            element_path += "MULTI\\"
            
            _meta_i2b2(c_hlevel, "", element_path, "MULTI", description, visualAttribute, current_timestamp, isModifier, appliedPath, datatypexml)
        ##Write INSERT statements for all notations.
        c_hlevel += 1
        visualAttribute = "LH"
        for i, notation in enumerate(notations):
            element_path_sub = element_path + i+"\\"
            notationPrefix = _getLongNotationPrefix(notation)
            notation = notationPrefix + notation
            
            _meta_i2b2(c_hlevel, notation, element_path_sub, displayLabel, description, visualAttribute, current_timestamp, isModifier, appliedPath, datatypexml)
            if isModifier:
                _demo_modifier(notation, element_path_sub, label, current_timestamp)
            else:
                _demo_concept(notation, element_path_sub, label, current_timestamp)


def _meta_i2b2(c_hlevel, notation, element_path, displayLabel, description, visualAttribute, current_timestamp, isModifier, appliedPath, datatypexml):
    """For i2b2metadata.i2b2 table"""
    pass

def _meta_table_access():
    """For i2b2metadata.table_access table"""
    pass

def _demo_modifier():
    """For i2b2demodata.modifier_dimension table"""
    pass

def _demo_concept():
    """For i2b2demodata.concept_dimension table"""
    pass

def _getShortNotationPrefix(notation:str) -> str:
    """Use mapping to get the shorthand prefix of the notation"""
    app.logger.info("Getting prefix for notation: {}".format(notation))
    notation_prefix = notation.split(":")[0]
    prefix = app.config["generator_mappings"][notation_prefix]
    app.logger.debug("Selected prefix: {}".format(prefix))
    return prefix

def _getLongNotationPrefix(notation:str) -> str:
    """Use mapping to get the full prefix of the notation"""
    app.logger.info("Getting prefix for notation: {}".format(notation))
    notation_prefix = notation.split(":")[0]
    prefixes = {v: k for k, v in app.config["generator_mappings"][notation_prefix]}
    prefix = prefixes[notation_prefix]
    app.logger.debug("Selected prefix: {}".format(prefix))
    return prefix