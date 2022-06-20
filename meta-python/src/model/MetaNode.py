""" meta_node.py
Object model for a metadata node
"""
from cmath import log
from flask import current_app as app

import logging
logger = logging.getLogger(__name__)

import datetime
from enum import Enum
import operator
import time

class NodeType(Enum):
    """Possible types of node"""
    CONCEPT = 1
    MODIFIER = 2
    COLLECTION = 3

class NodeDatatype(Enum):
    """Possible datatypes of node"""
    FLOAT = 1
    INTEGER = 2
    LARGESTRING = 3
    STRING = 4
    PARTIAL_DATE = 5
    DATE = 6

class NodeStatus(Enum):
    """Possible status of node"""
    DRAFT = 1


class MetaNode(object):
    """All CoMetaR nodes. Those with a notation will be extended by ConceptNode or ModifierNode
    Attributes can always be tagged, so we represent each attribute type as a dictionary of each occurance with the key being the attribute contents and the value being the tag.
    eg
    skos:prefLabel "Gesundheitsfragebogen: EQ-5D VAS"@de ;
    skos:prefLabel "Health questionnaire EQ-5D VAS"@en ;
    pref_labels = {"Gesundheitsfragebogen: EQ-5D VAS": "de", "Health questionnaire EQ-5D VAS": "en"}
    """
    ## TODO: Restrict length of strings when PostgreSQL column is of limited length

    parent_node = None
    child_nodes:list = None

    ## Used as title  (k is title, v is tag)
    pref_labels:dict[str:str] = None
    ## Used in sidebar/tree  (k is label, v is tag)
    _display_labels:dict[str:str] = None
    ## Main body text for node  (k is text, v is tag)
    descriptions:dict[str:str] = None
    ## Codes (k is notation, v is tag)
    _notations:dict[str:str] = None
    ## Optionnaly displayed in CoMetaR top right  (k is label, v is tag)
    alt_labels:dict[str:str] = None

    ## i2b2hidden
    dwh_display_status:str = None
    ## Where a modifier is applicable
    _applied_path:str = None
    ## When the object is created
    fetch_timestamp:str = None
    _node_type:NodeType = None
    status:NodeStatus = None
    _datatype:NodeDatatype = None
    ## Can be multiple units listed in CoMetaR - can (optionally) be tagged eg as UCUM, SI, etc
    ## TODO: Possibly use a Unit class?
    units:dict[str:str] = None

    @property
    def top_level_node(self) -> bool:
        """ Dynamically calculated
        Dependant on parent not existing"""
        if self.parent_node is None:
            # logger.info("Node '{}' is a top level element, it doesn't have any parents".format(self.name))
            return True
        else:
            return False
    @property
    def ancestor_count(self) -> int:
        """ Dynamically calculated
        Dependant on parent (eventually) not existing"""
        if self.parent_node is None:
            return 0
        else:
            return self.parent_node.ancestor_count + 1

    @property
    def c_table_cd(self) -> str:
        """Build the unique identifier for the tree of a top level node"""
        if self.top_level_node:
            ## Don't put the sourcesystem_cd in at this point, only when manipulating the CSV data
            # unique_code = "i2b2_{}_{}".format(self.sourcesystem_cd, self.concept_long_hash8)
            unique_code = "i2b2_{}".format(self.concept_long_hash8)
        else:
            unique_code = None
        logger.debug("Returning c_table_cd for '{}': {}".format(self.name, unique_code))
        return unique_code
    @property
    def visual_attribute(self) -> str:
        """ Dynamically calculated based on node_type and dwh_display
        Coded display charachteristics for i2b2 tree"""
        if self.node_type is NodeType.COLLECTION:
            va_part1 = "C"
        elif self.child_nodes and len(self.child_nodes) > 0:
            if self.node_type == NodeType.MODIFIER:
                va_part1 = "D"
            else:
                va_part1 = "F"
        elif not self.notations or len(self.notations) <= 1:
            if self.node_type == NodeType.MODIFIER:
                va_part1 = "R"
            else:
                va_part1 = "L"
        else:
            va_part1 = "M"

        if self.dwh_display_status and self.dwh_display_status.lower() == "i2b2hidden":
            va_part2 = "H"
        else:
            va_part2 = "A"
        if self.top_level_node:
            return "FA"
        return va_part1 + va_part2
    @property
    def element_path(self) -> str:
        """Dynamically calculated"""
        sep = app.config["i2b2_path_separator"]
        ipp = app.config["i2b2_path_prefix"]
        np = self.name
        if self.top_level_node:
            built_path = "{sep}{ipp}{sep}{np}{sep}".format(ipp = ipp, np = np, sep = sep)
        elif self.node_type == NodeType.MODIFIER and self.parent_node.node_type != NodeType.MODIFIER:
            ## "Parent" modifier
            built_path = "{sep}{np}{sep}".format(np = np, sep = sep)
        else:
            pnp = self.parent_node.element_path
            built_path = "{pnp}{sep}{np}{sep}".format(pnp = pnp, np = np, sep = sep)
        return built_path.replace("\\\\", "\\").replace("//", "/")
    @property
    def c_hlevel(self) -> int:
        """Dynamically calculated"""
        if self.node_type == NodeType.MODIFIER:
            if self.parent_node.node_type == NodeType.CONCEPT:
                return 1
            else:
                return self.parent_node.c_hlevel + 1
        else:
            return self.ancestor_count + 2
    @property
    def pref_labels(self) -> dict:
        """Dict or None"""
        if self._pref_labels and len(self._pref_labels) > 0:
            return self._pref_labels
        else:
            return None
    @property
    def pref_label(self) -> str:
        """ Get English pref_label (or German if no English) """
        # logger.debug("Searching for the right display label from dict: {}".format(self.pref_labels))
        if not self.pref_labels or len(self.pref_labels) == 0:
            single_label = ""
        elif "en" in self.pref_labels.values():
            keys_with_en = [k for k, v in self.pref_labels.items() if v == "en"]
            if len(keys_with_en) == 1:
                single_label = keys_with_en[0]
            else:
                logger.warn("More than 1 @en pref labels! '{}'".format(self.pref_labels))
                single_label = keys_with_en[0]
        else:
            single_label = self.pref_labels.keys()[0]
        # logger.debug("Decided on pref_label: {}".format(single_label))
        return single_label
    @pref_labels.setter
    def pref_labels(self, pref_labels):
        """Ensure no "None" key in dict"""
        self._pref_labels = pref_labels
        self._pref_labels.pop(None, None)

    @property
    def datatype(self) -> NodeDatatype:
        """The raw Enum representation of the datatype"""
        return self._datatype
    @datatype.setter
    def datatype(self, dtype:str):
        """Convert incoming string based indication to enum"""
        types = {NodeDatatype.INTEGER: ["int", "integer"], NodeDatatype.FLOAT: ["float", "dec", "decimal"], NodeDatatype.STRING: ["string", "str"], NodeDatatype.LARGESTRING: ["largestring"], NodeDatatype.PARTIAL_DATE: ["partial date", "partialDate", "partialDateRestriction"], NodeDatatype.DATE: ["dateRestriction", "date"]}
        ## Reverse the "types" dict so we can easily lookup the notations we expect to receive
        incoming_type_representations = {v.lower():k for k, l in types.items() for v in l}
        # logger.debug("Reversed types dict: {}\n \
        #     Type for node '{}': {}\
        # ".format(
        #     incoming_type_representations,
        #     self.name,
        #     dtype
        #     ))
        if dtype and dtype.lower() in incoming_type_representations.keys():
            self._datatype = incoming_type_representations.get(dtype.lower(), None)
        else:
            logger.warn("Trying to set node '{}' with an invalid datatype: {}".format(self.name, dtype))
            self._datatype = None
        # logger.debug("datetype for node '{}' set to: {}".format(self.name, self._datatype))
    @property
    def datatype_pretty(self) -> str:
        """ String version of datatype. With correct case for i2b2 """
        pretty_dt = ""
        if self.datatype is not None:
            dt_prettify = {NodeDatatype.INTEGER: "Integer", NodeDatatype.FLOAT: "Float",NodeDatatype.STRING: "String", NodeDatatype.LARGESTRING: "largeString", NodeDatatype.PARTIAL_DATE: "String", NodeDatatype.DATE: "String"}
            pretty_dt = dt_prettify[self.datatype]
        return pretty_dt
    @property
    def datatype_xml(self) -> str:
        """ XML string for datatype """
        xml_dt = "NULL"
        if self.datatype is not None:
            xml_dt = "<ValueMetadata><Version>3.02</Version><CreationDateTime>{fetch_timestamp}</CreationDateTime><DataType>{datatype_pretty}</DataType><Oktousevalues>Y</Oktousevalues>{units_xml}</ValueMetadata>".format(
                fetch_timestamp=self.fetch_timestamp, datatype_pretty=self.datatype_pretty, units_xml = self.units_xml
                )
        return xml_dt

    @property
    def units_xml(self) -> str:
        """XML section of ValueMetaData for unit"""
        xml_units = ""
        if self.units is not None and len(self.units) > 0:
            if self.datatype in [NodeDatatype.INTEGER, NodeDatatype.FLOAT]:
                first_item = True
                xml_units = "<UnitValues>"
                for unit_name in self.units.keys():
                    if first_item == True:
                        xml_units += "<NormalUnits>{}</NormalUnits>".format(unit_name)
                        first_item = False
                    else:
                        xml_units += "<EqualUnits>{}</EqualUnits>".format(unit_name)
                xml_units += "</UnitValues>"
                logger.debug("Units for node '{}': {}".format(self.name, xml_units))
        return xml_units

    @property
    def c_facttablecolumn(self) -> str:
        """Which column in the fact table"""
        if self.node_type == NodeType.MODIFIER:
            return "modifier_cd"
        else:
            return "concept_cd"
    @property
    def c_tablename(self) -> str:
        """Which table in the data schema"""
        if self.node_type == NodeType.MODIFIER:
            return "modifier_dimension"
        else:
            return "concept_dimension"
    @property
    def c_columnname(self) -> str:
        """Which column in the fact table"""
        if self.node_type == NodeType.MODIFIER:
            return "modifier_path"
        else:
            return "concept_path"
    @property
    def applied_path(self) -> str:
        """Path where the modifier is applicable"""
        if self.node_type != NodeType.MODIFIER:
            ## TODO: Define this return string in a config
            return "@"
        elif self.parent_node.node_type == NodeType.MODIFIER:
            return self.parent_node.applied_path
        else:
            ## NEW - START
            ## No need to include the multi-notation container as that would make the modifier also hidden
            new_path = "{parent_path}{sep}%".format(
                parent_path = self.parent_node.element_path,
                sep = app.config["i2b2_path_separator"]
                ).replace("\\\\", "\\").replace("//", "/")
            ## NEW - END
            ## OLD - START
            if self.parent_node.notations and type(self.parent_node.notations) is dict and len(self.parent_node.notations) > 1:
            # if self.parent_node.child_nodes and len(self.parent_node.child_nodes) > 1:
                new_path = "{parent_path}{sep}{impc}{sep}%".format(
                    parent_path = self.parent_node.element_path,
                    impc = app.config["i2b2_multipath_container"],
                    sep = app.config["i2b2_path_separator"]
                    ).replace("\\\\", "\\").replace("//", "/")
                logger.debug("Adding MULTI to applied_path for {}: {}".format(self.name, new_path))
            else:
                new_path = "{parent_path}{sep}%".format(
                    parent_path = self.parent_node.element_path,
                    sep = app.config["i2b2_path_separator"]
                    ).replace("\\\\", "\\").replace("//", "/")
            ## OLD - END

            ## TODO: For consistency with existing system - START REMOVE
            if "12645001" in self.concept_long:
                logger.debug("Parent notations for amplification modifier (12645001): {}".format(self.parent_node.notations))
            # if (self.parent_node.notations and type(self.parent_node.notations) is dict and len(self.parent_node.notations) > 1) \
            #     or (self.parent_node.child_nodes and len(self.parent_node.child_nodes) > 1):
            # if self.parent_node.child_nodes and len(self.parent_node.child_nodes) > 1:
            #     logger.debug("Adding MULTI to applied_path for {}".format(self.name))
            #     new_path.replace("\\%", "\\MULTI\\%")
            ## TODO: For consistency with existing system - END REMOVE
            return new_path

    @property
    def display_labels(self) -> dict:
        """Dict or None
        (k is label, v is language tag)
        """
        if self._display_labels and len(self._display_labels) > 0:
            return self._display_labels
        else:
            return None
    @property
    def display_label(self) -> str:
        """ Get English display_label (or German if no English) """
        logger.debug("Searching for the right display label for '{}' from dict: {}".format(self.name, self.display_labels))
        if not self.display_labels or len(self.display_labels) == 0:
            # logger.debug("Empty display label")
            single_label = ""
        elif len(self.display_labels) == 1:
            ## When there is no choice, no need to do more complex processing
            single_label = list(self.display_labels.keys())[0]
        elif "en" in self.display_labels.values():
            keys_with_en = [k for k, v in self.display_labels.items() if v == "en"]
            # if operator.countOf(self.display_labels.values(), "en") == 1:
            logger.debug("en keys: {}".format(keys_with_en))
            single_label = keys_with_en[0]
            if len(keys_with_en) > 1:
                single_label = keys_with_en[0]
                # single_label = keys_with_en[-1]
                logger.warn("More than 1 @en display_label ({}) for {}! Chosen: '{}'".format(self.display_labels, self.name, single_label))
        else:
            # logger.debug("First display label  in dict")
            single_label = self.display_labels.keys()[0]
        # logger.debug("Decided on display_label: {}".format(single_label))
        return single_label
    @display_labels.setter
    def display_labels(self, display_labels):
        """Simple"""
        self._display_labels = display_labels
        ## Ensure there is not a "None" key
        self._display_labels.pop(None, None)

    @property
    def description(self) -> str:
        """Return the first description as a string"""
        if self.descriptions is None or len(self.descriptions) == 0:
            return self.pref_label
        else:
            return next(iter(self.descriptions))

    @property
    def concept_long(self) -> str:
        """Concept path"""
        return self.element_path
    @property
    def concept_long_hash8(self) -> str:
        """Concept path"""
        import base64
        import hashlib
        hasher = hashlib.sha1(self.element_path.encode()).digest()
        # hash8 = base64.urlsafe_b64encode(hasher.digest()[:8])
        hash8 = base64.urlsafe_b64encode(hasher[:8]).decode('ascii')[:8]
        return hash8

    @property
    def notations(self) -> dict:
        """Return notation objects or single notation string"""
        # logger.debug("Child nodes for '{}': {}".format(self.name, self.child_nodes))
        if self.child_nodes is not None and len(self.child_nodes) != 0 and self._notations is not None and len(self._notations) > 1:
            ## Create the multi container path if needed (when notations could clash with child nodes)
            dummy_notation = {app.config["i2b2_multipath_container"]: NotationNode(self, None)}
            return {**dummy_notation, **self._notations}
        else:
            return self._notations
    @property
    def notation(self) -> str:
        """Return the notation for the base Node (only a real notation if there exists only 1 notation, otherwise its empty)"""
        if self.notations is not None and len(self.notations) == 1:
            return list(self.notations.keys())[0]
        else:
            return ""
    @notations.setter
    def notations(self, notations:dict):
        """Create objects from name(s) and tag
        NOTE: The empty "multi" container is not created or stored, but generated when notations are retrieved. Safer in case child_nodes are added later
        """
        if notations is None or len(notations) == 0:
            ## No notations
            self._notations = None
        elif len(notations) == 1:
            ## Simple notation
            self._notations = notations
        else:
            ## Multi notations - NOTE: repeated logic must be matched in NotationNode to calculate the correct path
            new_notations = {}
            for notation_name, notation_tag in notations.items():
                ## For the actual notations
                new_notations[notation_name] = NotationNode(self, notation_name, notation_tag)
            self._notations = new_notations

    @property
    def node_type(self) -> NodeType:
        """Return raw node_type enum"""
        return self._node_type
    @property
    def node_type_pretty(self) -> str:
        """Return a string representation of the node_type"""
        return self._node_type._name_.lower()
    @node_type.setter
    def node_type(self, node_type:str):
        """Set node_type enum NodeType"""
        if self.parent_node is not None and self.parent_node.node_type is NodeType.MODIFIER:
            self._node_type = NodeType.MODIFIER
        elif node_type is None:
            logger.error("Node ({}) must have a type, not None!".format(self.name))
        elif node_type.lower() in ["concept"]:
            self._node_type = NodeType.CONCEPT
        elif node_type.lower() in ["modifier"]:
            self._node_type = NodeType.MODIFIER
        elif node_type.lower() in ["collection"]:
            self._node_type = NodeType.COLLECTION
        else:
            logger.error("Node ({}) must have a type! {}".format(self.name, node_type))

    @property
    def meta_csv(self) -> dict:
        """csv data with same format as i2b2metadata i2b2 and table_access tables"""
        d = self.__dict__()
        # logger.debug("computed members: {}".format(d))
        i2b2_cols = ["c_hlevel", "c_fullname", "c_name", "c_synonym_cd", "c_visualattributes", "c_totalnum", "c_basecode", "c_metadataxml", "c_facttablecolumn", "c_tablename", "c_columnname", "c_columndatatype", "c_operator", "c_dimcode", "c_comment", "c_tooltip", "m_applied_path", "update_date", "download_date", "import_date", "sourcesystem_cd", "valuetype_cd", "m_exclusion_cd", "c_path", "c_symbol"]
        ## meta_inserts' dict can have 2 entries, i2b2 and table_access
        lines = {"i2b2": [], "table_access": []}
        ## ontology can have multiple entries when there are multiple notations
        ## Always insert the base node
        lines["i2b2"].append(MetaNode._data_to_csv(ordered_cols = i2b2_cols, d = d, table_name = "i2b2", schema_name = "i2b2metadata"))
        logger.debug("self.notations for '{}': {}".format(self.name, self.notations))
        if self.notations and len(self.notations) >= 2:
            ## If multiple notations, use NotationNode objects to populate the additional csv lines
            for notation_obj in self.notations.values():
                lines["i2b2"].append(MetaNode._data_to_csv(ordered_cols = i2b2_cols, d = notation_obj.__dict__(), table_name = "i2b2", schema_name = "i2b2metadata"))
            simplified_lines = {}
            simplified_lines["i2b2"] = [v[0:6] for v in lines["i2b2"]]
            logger.debug("Multiple notations for '{}'...\n{}".format(self.name, simplified_lines))

        ta_cols = ["c_table_cd", "c_table_name", "c_protected_access", "c_ontology_protection", "c_hlevel", "c_fullname", "c_name", "c_synonym_cd", "c_visualattributes", "c_totalnum", "c_basecode", "c_metadataxml", "c_facttablecolumn", "c_dimtablename", "c_columnname", "c_columndatatype", "c_operator", "c_dimcode", "c_comment", "c_tooltip", "c_entry_date", "c_change_date", "c_status_cd", "valuetype_cd"]
        if self.top_level_node:
            d["c_hlevel"] = 1
            logger.debug("Using self dict: {}".format(d))
            lines["table_access"] = [MetaNode._data_to_csv(ordered_cols = ta_cols, d = d, table_name = "table_access", schema_name = "i2b2metadata")]
        # else:
        #     lines["table_access"] = None
        return lines
    @property
    def data_csv(self) -> dict:
        """csv data with same format as i2b2demodata concept- or modifier- dimension tables"""
        if not self.notations or len(self.notations) == 0:
            ## When this is just a container/folder, there is nothing to do
            return None
        d = self.__dict__()
        # logger.debug("computed members: {}".format(d))

        if self.node_type == NodeType.CONCEPT:
            concept_type_table = "concept_dimension"
            cols = ["concept_path", "concept_cd", "name_char", "concept_blob", "update_date", "download_date", "import_date", "sourcesystem_cd", "upload_id"]
        elif self.node_type == NodeType.MODIFIER:
            concept_type_table = "modifier_dimension"
            cols = ["modifier_path", "modifier_cd", "name_char", "modifier_blob", "update_date", "download_date", "import_date", "sourcesystem_cd", "upload_id"]
        else:
            logger.error("This node ({}) should be a concept or modifier, its niether! {}".format(self.node_uri, self.node_type))
            return None
        ## data inserts occur once for each notation, but not for the containing concept (unless its a single notation)
        lines = {"concept_dimension": [], "modifier_dimension": []}
        if len(self.notations) == 1:
            lines[concept_type_table].append(MetaNode._data_to_csv(ordered_cols = cols, d = d, table_name = concept_type_table, schema_name = "i2b2demodata"))
        else:
            for notation_obj in self.notations.values():
                if notation_obj.notation is not None and notation_obj.notation != "":
                    lines[concept_type_table].append(MetaNode._data_to_csv(ordered_cols = cols, d = notation_obj.__dict__(), table_name = concept_type_table, schema_name = "i2b2demodata"))
        return lines

    def __init__(self, node_uri, name, node_type, pref_labels, display_labels, notations, descriptions, alt_labels = None, datatype = None, dwh_display_status = None, parent_node = None, units = None, sourcesystem_cd = "UNKNOWN") -> None:
        """Initialise an instance with data"""
        if parent_node is not None:
            self.parent_node = parent_node
            self.parent_node.add_child(self)
            logger.debug("Node has parent ({}): {}".format(self.parent_node, self.parent_node.node_uri))
        self.child_nodes = []
        self.node_uri = node_uri
        self.name = name
        self.node_type = node_type
        self.pref_labels = pref_labels
        self.display_labels = display_labels
        self.notations = notations
        self.descriptions = descriptions
        self.alt_labels = alt_labels
        self.datatype = datatype
        self.units = units
        self.dwh_display_status = dwh_display_status
        self.sourcesystem_cd = sourcesystem_cd
        ## TODO: take time format from config
        self.fetch_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.0")
        logger.info("New MetaNode object created! ({})".format(self.node_uri))

    def add_child(self, child_node) -> None:
        """Add a child of this node to the list"""
        logger.debug("Adding child: {}".format(child_node))
        self.child_nodes.append(child_node)
        ## Deduplicate:
        self.child_nodes = list(set(self.child_nodes))

    def whole_tree_csv(self, lines:dict = None) -> dict:
        """dict with 4 lists for each table in: i2b2metadata{table_access,i2b2}, i2b2demodata{concept_dimension,modifier_dimension}"""
        logger.info("Adding csv lines for '{}' ({}): {}".format(self.name, self.node_type_pretty, self.node_uri))
        # time.sleep(4)
        if lines is None:
            lines = {"i2b2metadata":{"table_access": [], "i2b2": []},"i2b2demodata":{"concept_dimension": [], "modifier_dimension": []}}
        if self.top_level_node:
            logger.debug("#### ~~~~ STARTING whole tree CSV ~~~~ ####")
            logger.debug("Starting point for meta_csv: {}".format(lines["i2b2metadata"]))
            logger.debug("Starting point for data_csv: {}".format(lines["i2b2demodata"]))
            # time.sleep(2)

        i2b2metadata_csv = self.meta_csv
        if i2b2metadata_csv is not None:
            if i2b2metadata_csv.get("table_access") is not None:
                lines["i2b2metadata"]["table_access"].extend(i2b2metadata_csv["table_access"])
            if i2b2metadata_csv.get("i2b2") is not None:
                lines["i2b2metadata"]["i2b2"].extend(i2b2metadata_csv["i2b2"])
        logger.debug("Added meta_csv: {}".format(i2b2metadata_csv))

        i2b2demodata_csv = self.data_csv
        if i2b2demodata_csv is not None:
            if i2b2demodata_csv.get("concept_dimension") is not None:
                lines["i2b2demodata"]["concept_dimension"].extend(i2b2demodata_csv["concept_dimension"])
            if i2b2demodata_csv.get("modifier_dimension") is not None:
                lines["i2b2demodata"]["modifier_dimension"].extend(i2b2demodata_csv["modifier_dimension"])
        logger.debug("Added data_csv: {}".format(i2b2demodata_csv))

        if self.child_nodes is not None and len(self.child_nodes) > 0:
            logger.debug("Getting CSV for children: {}".format(self.child_nodes))
            for child in self.child_nodes:
                lines = child.whole_tree_csv(lines)
        if self.top_level_node:
            logger.debug("#### ~~~~ FINISHED whole tree CSV ~~~~ ####")
            # logger.debug("Finished meta_csv: {}".format(lines["i2b2metadata"]))
            # logger.debug("Finished data_csv: {}".format(lines["i2b2demodata"]))
            logger.debug("Finished meta_csv - (i2b2: {}) (table_access: {})".format(len(lines["i2b2metadata"]["i2b2"]), len(lines["i2b2metadata"]["table_access"])))
            logger.debug("Finished data_csv - (concept_dimension: {}) (modifier_dimension: {})".format(len(lines["i2b2demodata"]["concept_dimension"]), len(lines["i2b2demodata"]["concept_dimension"])))
            logger.debug("#### ~~~~ FINISHED whole tree CSV ~~~~ ####")
        return lines

    @classmethod
    def _data_to_csv(cls, ordered_cols:list, d:dict, delim:str = ";", table_name:str = None, schema_name:str = None) -> list:
        """Generate a csv line given the columns and data provided"""
        first = True
        line = []
        d["ontology_tablename"] = app.config["ontology_tablename"]
        for col_name in ordered_cols:
            new_value = ""
            real_property_options = None
            real_property = None
            ## Inject fixed values for some columns (see sql inserts) OR
            ## Lookup name map as sql cols can differ from attribute/property names in code. Also can implement preference list to avoid empty values
            if schema_name and table_name and "{sn}{sep}{tn}{sep}{cn}".format(sep="-", sn=schema_name, tn=table_name, cn=col_name) in app.config["fixed_value_cols"]:
                new_value = str(app.config["fixed_value_cols"].get("{sn}{sep}{tn}{sep}{cn}".format(sep="-", sn=schema_name, tn=table_name, cn=col_name), ""))
            elif schema_name and table_name and "{sn}{sep}{tn}{sep}{cn}".format(sep="-", sn=schema_name, tn=table_name, cn=col_name) in app.config["sql_col_object_property_map"]:
                # logger.info("Mapping SQL column '{}' to object attribute '{}'".format(col_name, [y for x,y in app.config["sql_col_object_property_map"].items() if col_name in x]))
                real_property_options = app.config["sql_col_object_property_map"].get("{sn}{sep}{tn}{sep}{cn}".format(sep="-", sn=schema_name, tn=table_name, cn=col_name), "")
            elif table_name and "{tn}{sep}{cn}".format(sep="-", tn=table_name, cn=col_name) in app.config["fixed_value_cols"]:
                new_value = str(app.config["fixed_value_cols"].get("{tn}{sep}{cn}".format(sep="-", tn=table_name, cn=col_name), ""))
            elif table_name and "{tn}{sep}{cn}".format(sep="-", tn=table_name, cn=col_name) in app.config["sql_col_object_property_map"]:
                real_property_options = app.config["sql_col_object_property_map"].get("{tn}{sep}{cn}".format(sep="-", tn=table_name, cn=col_name), "")
            elif col_name in app.config["fixed_value_cols"]:
                new_value = str(app.config["fixed_value_cols"].get(col_name, ""))
            elif col_name in app.config["sql_col_object_property_map"]:
                real_property_options = app.config["sql_col_object_property_map"].get(col_name, "")
                # logger.debug("Mapping SQL col '{}' from fuseki properties in map '{}' (Type: {})".format(col_name, real_property_options, type(real_property_options)))
            else:
                new_value = str(d.get(col_name, ""))
                logger.debug("Matched and available col_name ({}) name for '{}', value: {}".format(col_name, d["pref_label"], new_value))
            if real_property_options and type(real_property_options) is str:
                real_property = real_property_options
            elif real_property_options and type(real_property_options) is list:
                real_property = ""
                for attempt_property in real_property_options:
                    if d.get(attempt_property, None) is not None and str(d.get(attempt_property, "")) != "":
                    # if attempt_property in d and str(d.get(attempt_property, "")) != "":
                        logger.debug("Found non-empty useful value with prop '{}': '{}'".format(attempt_property, str(d.get(attempt_property, ""))))
                        real_property = attempt_property
                        break
                    else:
                        logger.debug("Found empty/useless value with prop '{}': {}".format(attempt_property, str(d.get(attempt_property, ""))))
                        pass
            if real_property:
                new_value = str(d.get(real_property, ""))
                logger.debug("mismatched sql({})/object({}) name for '{}', value: {}".format(col_name, real_property, d["pref_label"], new_value))
            if first:
                first = False
                line = [new_value]
            else:
                line.append(new_value)
        return line

    def __dict__(self):
        """Return all properties which are useful as well as any regular attributes"""
        all_attributes = [
            "c_table_cd", "c_hlevel", "concept_long", "concept_long_hash8", "pref_label", "visual_attribute", "datatype_xml", "c_facttablecolumn", "c_tablename", "c_columnname",
            "description", "descriptions", "applied_path", "fetch_timestamp", "notation", "notations", "sourcesystem_cd", "display_label"
        ]
        d = {k: getattr(self, k, None) for k in all_attributes}
        return d


class NotationNode(object):
    """Sometimes we have multiple notations, each needs a node in i2b2 but is mostly inherited from the parent concept"""

    @property
    def visual_attribute(self) -> str:
        """For this niche category, its "MH" for the container or "LH" for the real notations (multi/leaf hidden)"""
        if self.notation == "":
            return "MH"
        else:
            return "LH"
    @property
    def element_path(self) -> str:
        """Dynamically calculated
            - multi container only used if node has children
            - index only used if node has multiple notations
        """
        # logger.debug("Checking parent node '{}' for element path stem: {}".format(self.containing_node, self.containing_node.element_path))
        sep = app.config["i2b2_path_separator"]
        impc = app.config["i2b2_multipath_container"]
        pnp = self.containing_node.element_path
        notation_path = ""
        if self.notation == "":
            notation_path = r"{pnp}{impc}{sep}".format(pnp = pnp, impc = impc, sep = sep)
        elif self.containing_node.child_nodes is None or len(self.containing_node.child_nodes) == 0:
            ## No multi-path hidden container when there are no child nodes
            notation_path = r"{pnp}{sep}{ni}{sep}".format(
                pnp = pnp,
                sep = sep,
                ni = list(self.containing_node.notations.values()).index(self)
                )
        else:
            notation_path = r"{pnp}{impc}{sep}{ni}{sep}".format(
                pnp = pnp,
                sep = sep,
                impc = impc,
                ni = list(self.containing_node.notations.values()).index(self) - 1
                )
        ## All of these objects are where there are multiple notations, so expect the empty container, all should have an index (included above)
        # if self.containing_node.notations and len(self.containing_node.notations) > 1:
        #     notation_path += r"{ni}{sep}".format(sep = sep, ni = list(self.containing_node.notations.values()).index(self))
        # logger.debug("Calculated notation path for '{}': {}".format(self.notation, notation_path))
        ## Remove duplicate slashes - if they they were at start and end of concatenated strings, then they will be doubled (back-slashes will be escaped too)
        return notation_path.replace("\\\\", "\\").replace("//", "/")
    @property
    def c_hlevel(self) -> int:
        """Dynamically calculated. \MULTI\ is +1, actual notations are +2"""
        ## TODO: or when containing_node notations length is 1?
        if self.notation == "" or self.containing_node.child_nodes is None or len(self.containing_node.child_nodes) == 0:
            ## Hidden container \MULTI\
            ## Or when no children
            return self.containing_node.c_hlevel + 1
        else:
            ## Should be when multi and index exist together - 
            return self.containing_node.c_hlevel + 2
    @property
    def notation(self) -> str:
        """Get the notation string for this level"""
        if self._notation is None:
            return ""
        else:
            return self._notation

    @property
    def display_label(self) -> str:
        """Let display_label be "MULTI" when this is the container node"""
        if self.visual_attribute == "MH":
            return "MULTI"
        else:
            return self.containing_node.display_label
    @property
    def concept_long(self) -> str:
        """Concept path"""
        # logger.debug("NOTATION! Getting concept_long/element_path '{}' for: {}".format(self.element_path, self.notation))
        return self.element_path
    @property
    def concept_long_hash8(self) -> str:
        """Concept path"""
        import base64
        import hashlib
        hasher = hashlib.sha1(self.element_path.encode()).digest()
        # hash8 = base64.urlsafe_b64encode(hasher.digest()[:8])
        hash8 = base64.urlsafe_b64encode(hasher[:8]).decode('ascii')[:8]
        return hash8

    def __init__(self, containing_node, notation = None, tag = None) -> None:
        """We want to know which instance is our parent, then we can extend it's attributes"""
        self.containing_node = containing_node
        self._notation = notation
        self.tag = tag

    def __dict__(self):
        """Return all properties which are useful as well as any regular attributes"""
        # parent_attributes = ["pref_label", "datatype_xml", "c_facttablecolumn", "c_tablename", "c_columnname", "description", "applied_path", "fetch_timestamp"]
        # notation_attributes = ["c_hlevel", "visual_attribute", "concept_long", "concept_long_hash8", "notation"]
        # all_attributes = [*parent_attributes, *notation_attributes]
        all_attributes = [
            "c_hlevel", "concept_long", "pref_label", "visual_attribute", "datatype_xml", "c_facttablecolumn", "c_tablename", "c_columnname", "description", "descriptions",
            "applied_path", "fetch_timestamp", "concept_long_hash8", "notation", "notations", "display_label"]
        d1 = {k: getattr(self.containing_node, k, None) for k in all_attributes if not hasattr(self, k)}
        d2 = {k: getattr(self, k, None) for k in all_attributes if hasattr(self, k)}
        return {**d1, **d2}

##End
