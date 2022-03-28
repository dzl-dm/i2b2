""" meta_node.py
Object model for a metadata node
"""
from flask import current_app as app

import logging
logger = logging.getLogger(__name__)

import datetime
from enum import Enum

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
    dwh_display:str = None
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
            unique_code = "i2b2_{}_{}".format(self.sourcesystem_cd, self.concept_long_hash8)
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

        if self.dwh_display == "i2b2hidden":
            va_part2 = "H"
        else:
            va_part2 = "A"
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
    def pref_label(self) -> str:
        """ Get English pref_label (or German if no English) """
        if not self.pref_labels or len(self.pref_labels) == 0:
            single_label = ""
        elif "en" in self.pref_labels.values():
            single_label = list(self.pref_labels.keys())[list(self.pref_labels.values()).index("en")]
        else:
            logger.debug("All keys for '{}': {}".format(self.node_uri, self.pref_labels.keys()))
            single_label = self.pref_labels.keys()[0]
        return single_label
    @property
    def datatype(self) -> NodeDatatype:
        """The raw Enum representation of the datatype"""
        return self._datatype
    @datatype.setter
    def datatype(self, dtype:str):
        """Convert incoming string based indication to enum"""
        types = {NodeDatatype.INTEGER: ["int", "integer"], NodeDatatype.FLOAT: ["float", "dec", "decimal"], NodeDatatype.STRING: ["string", "str"], NodeDatatype.LARGESTRING: ["largestring"]}
        incoming_types = {v:k for k, l in types.items() for v in l}
        if dtype in incoming_types.keys():
            self._datatype = incoming_types.get(dtype.lower(), None)
        else:
            logger.warn("Trying to set node '{}' with an invalid datatype: {}".format(self.name, dtype))
            self._datatype = None
    @property
    def datatype_pretty(self) -> str:
        """ String version of datatype. With correct case for i2b2 """
        pretty_dt = ""
        if self.datatype is not None:
            dt_prettify = {NodeDatatype.INTEGER: "Integer", NodeDatatype.FLOAT: "Float",NodeDatatype.STRING: "String", NodeDatatype.LARGESTRING: "largeString"}
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
            ## Must be the parent modifier of a tree (or a standalone modifier)
            return "{parent_path}{sep}%".format(
                parent_path = self.parent_node.element_path,
                sep = app.config["i2b2_path_separator"]
                ).replace("\\\\", "\\").replace("//", "/")

    @property
    def display_labels(self) -> dict:
        """Default to pref_labels"""
        if self._display_labels and len(self._display_labels) > 0:
            return self._display_labels
        else:
            return self.pref_labels
    @property
    def display_label(self) -> str:
        """ Get English display_label (or German if no English) """
        if not self.display_labels or len(self.display_labels) == 0:
            single_label = ""
        elif "en" in self.display_labels.values():
            single_label = self.display_labels.keys()[self.display_labels.values().index("en")]
        else:
            single_label = self.display_labels.keys()[0]
        return single_label
    @display_labels.setter
    def display_labels(self, display_labels):
        """Simple"""
        self._display_labels = display_labels

    @property
    def description(self) -> str:
        """Return the first description as a string"""
        if self.descriptions is None or len(self.descriptions) == 0:
            return ""
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
    def meta_inserts(self) -> list:
        """SQL inserts for the i2b2metadata i2b2 (and table_access dependant on need)"""
        d = self.__dict__()
        # logger.debug("computed members: {}".format(d))

        ## meta_inserts' dict can have 2 entries, ontology and (optionally) table_access
        inserts = {}
        ## ontology can have multiple entries when there
        inserts["ontology"] = []
        ## Always insert the base node
        inserts["ontology"].append(MetaNode._single_ontology_insert(d = d, meta_schema = app.config["meta_schema"], ontology_tablename = app.config["ontology_tablename"], sourcesystem = app.config["sourcesystem"]))
        if self.notations and len(self.notations) >= 2:
            ## If multiple notations, use NotationNode objects to populate the additional INSERT's
            for notation_obj in self.notations.values():
                inserts["ontology"].append(MetaNode._single_ontology_insert(d = notation_obj.__dict__(), meta_schema = app.config["meta_schema"], ontology_tablename = app.config["ontology_tablename"], sourcesystem = app.config["sourcesystem"]))

        if self.top_level_node:
            inserts["table_access"] = MetaNode._table_access_insert(d = d, meta_schema = app.config["meta_schema"], ontology_tablename = app.config["ontology_tablename"])
        else:
            inserts["table_access"] = None
        
        if inserts["table_access"] is not None:
            all_inserts = [inserts["table_access"], *inserts["ontology"]]
        else:
            all_inserts = inserts["ontology"]
        return all_inserts
    @property
    def data_inserts(self) -> list:
        """SQL Insterts for the i2b2demodata - concept OR modifier"""
        if not self.notations or len(self.notations) == 0:
            ## When this is just a container/folder, there is nothing to do
            return None
        d = self.__dict__()
        # logger.debug("computed members: {}".format(d))

        if self.node_type == NodeType.CONCEPT:
            concept_type_table = "concept_dimension"
        elif self.node_type == NodeType.MODIFIER:
            concept_type_table = "modifier_dimension"
        else:
            logger.error("This node ({}) should be a concept or modifier, its niether! {}".format(self.node_uri, self.node_type))
            return None
        ## data inserts occur once for each notation, but not for the containing concept (unless its a single notation)
        inserts = []
        if len(self.notations) == 1:
            inserts.append(MetaNode._concept_insert(d = d, data_schema = app.config["data_schema"], concept_type_table = concept_type_table, sourcesystem = app.config["sourcesystem"]))
        else:
            for notation_obj in self.notations.values():
                if notation_obj.notation is not None and notation_obj.notation != "":
                    inserts.append(MetaNode._concept_insert(d = notation_obj.__dict__(), data_schema = app.config["data_schema"], concept_type_table = concept_type_table, sourcesystem = app.config["sourcesystem"]))
        return inserts

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
        lines["i2b2"].append(MetaNode._data_to_csv(ordered_cols = i2b2_cols, d = d))
        logger.debug("self.notations for '{}': {}".format(self.name, self.notations))
        if self.notations and len(self.notations) >= 2:
            ## If multiple notations, use NotationNode objects to populate the additional csv lines
            for notation_obj in self.notations.values():
                lines["i2b2"].append(MetaNode._data_to_csv(ordered_cols = i2b2_cols, d = notation_obj.__dict__()))
            logger.debug("Multiple notations for '{}'...\n{}".format(self.name, lines))

        ta_cols = ["c_table_cd", "c_table_name", "c_protected_access", "c_ontology_protection", "c_hlevel", "c_fullname", "c_name", "c_synonym_cd", "c_visualattributes", "c_totalnum", "c_basecode", "c_metadataxml", "c_facttablecolumn", "c_dimtablename", "c_columnname", "c_columndatatype", "c_operator", "c_dimcode", "c_comment", "c_tooltip", "c_entry_date", "c_change_date", "c_status_cd", "valuetype_cd"]
        if self.top_level_node:
            d["c_hlevel"] = 1
            logger.debug("Using self dict: {}".format(d))
            lines["table_access"] = [MetaNode._data_to_csv(ordered_cols = ta_cols, d = d)]
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
            lines[concept_type_table].append(MetaNode._data_to_csv(ordered_cols = cols, d = d))
        else:
            for notation_obj in self.notations.values():
                if notation_obj.notation is not None and notation_obj.notation != "":
                    lines[concept_type_table].append(MetaNode._data_to_csv(ordered_cols = cols, d = notation_obj.__dict__()))
        return lines

    def __init__(self, node_uri, name, node_type, pref_labels, display_labels, notations, descriptions, alt_labels = None, datatype = None, display_status = None, parent_node = None, units = None, sourcesystem_cd = "UNKNOWN") -> None:
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
        self.display_status = display_status
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

    def whole_tree_inserts(self, inserts:dict = None) -> dict:
        """2 lists of insert statements, one for each schema (meta/demodata)"""
        logger.info("Adding SQL statements for '{}' ({}): {}".format(self.name, self.node_type_pretty, self.node_uri))
        if inserts is None:
            inserts = {"meta":[],"data":[]}
        # if self.name == ":DemoData":
        if self.top_level_node:
            logger.debug("#### ~~~~ STARTING whole tree SQL ~~~~ ####")
            logger.debug("Starting point for meta_sql: {}".format(inserts["meta"]))
            logger.debug("Starting point for data_sql: {}".format(inserts["data"]))

        meta_sql = self.meta_inserts
        if meta_sql is not None:
            inserts["meta"].extend(meta_sql)
        # logger.debug("Added meta_sql: {}".format(meta_sql))

        data_sql = self.data_inserts
        if data_sql is not None:
            inserts["data"].extend(data_sql)
        # logger.debug("Added data_sql: {}".format(data_sql))

        if self.child_nodes is not None and len(self.child_nodes) > 0:
            # logger.debug("Getting SQL for children: {}".format(self.child_nodes))
            for child in self.child_nodes:
                inserts = child.whole_tree_inserts(inserts)
        # if self.name == ":DemoData":
        if self.top_level_node:
            logger.debug("#### ~~~~ FINISHED whole tree SQL ~~~~ ####")
            logger.debug("Finished meta_sql: {}".format(inserts["meta"]))
            logger.debug("Finished data_sql: {}".format(inserts["data"]))
            logger.debug("#### ~~~~ FINISHED whole tree SQL ~~~~ ####")
        return inserts

    def whole_tree_csv(self, lines:dict = None) -> dict:
        """dict with 4 lists for each table in: i2b2metadata{table_access,i2b2}, i2b2demodata{concept_dimension,modifier_dimension}"""
        logger.info("Adding csv lines for '{}' ({}): {}".format(self.name, self.node_type_pretty, self.node_uri))
        if lines is None:
            lines = {"i2b2metadata":{"table_access": [], "i2b2": []},"i2b2demodata":{"concept_dimension": [], "modifier_dimension": []}}
        if self.top_level_node:
            logger.debug("#### ~~~~ STARTING whole tree CSV ~~~~ ####")
            logger.debug("Starting point for meta_csv: {}".format(lines["i2b2metadata"]))
            logger.debug("Starting point for data_csv: {}".format(lines["i2b2demodata"]))

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
            logger.debug("Finished meta_csv: {}".format(lines["i2b2metadata"]))
            logger.debug("Finished data_csv: {}".format(lines["i2b2demodata"]))
            logger.debug("#### ~~~~ FINISHED whole tree CSV ~~~~ ####")
        return lines

    @classmethod
    def _single_ontology_insert(cls, d, meta_schema, ontology_tablename, sourcesystem) -> str:
        """Return SQL line for single notation insert to ontology table"""
        d["meta_schema"] = meta_schema
        d["ontology_tablename"] = ontology_tablename
        d["sourcesystem"] = sourcesystem
        if d["notation"] == "":
            d["notation"] = "NULL"
        # logger.debug("Using dict for sql writing: {}".format(d))
        notation_sql = """INSERT INTO {meta_schema}.{ontology_tablename} (
            c_hlevel,c_fullname,c_name,c_synonym_cd,c_visualattributes,
            c_basecode,c_metadataxml,c_facttablecolumn,c_tablename,c_columnname,
            c_columndatatype,c_operator,c_dimcode,c_tooltip,m_applied_path,
            update_date,download_date,import_date,sourcesystem_cd)
        VALUES(
            {c_hlevel},'{concept_long}','{pref_label}','N','{visual_attribute}',
            '{notation}','{datatype_xml}','{c_facttablecolumn}','{c_tablename}','{c_columnname}',
            'T','LIKE','{concept_long}','{description}','{applied_path}',
            current_timestamp,'{fetch_timestamp}',current_timestamp,'{sourcesystem}'
        );\n""".format(**d)
        notation_sql = notation_sql.replace("\t", " ").replace("\n", " ")
        ## Combine multiple spaces to single space
        notation_sql = " ".join(notation_sql.split())
        return notation_sql
    @classmethod
    def _table_access_insert(cls, d, meta_schema, ontology_tablename) -> str:
        """Return SQL line for table access entry"""
        d["meta_schema"] = meta_schema
        d["ontology_tablename"] = ontology_tablename
        # logger.debug("Using dict for sql writing: {}".format(d))
        top_level_sql = """INSERT INTO {meta_schema}.table_access (
					c_table_cd,c_table_name,c_protected_access,c_hlevel,c_fullname,
					c_name,c_synonym_cd,c_visualattributes,c_facttablecolumn,c_dimtablename,
					c_columnname,c_columndatatype,c_operator,c_dimcode,c_tooltip
				) VALUES(
					'i2b2_{sourcesystem_cd}_{concept_long_hash8}','{ontology_tablename}','N',1,'{concept_long}',
					'{pref_label}','N','{visual_attribute}','concept_cd','concept_dimension',
					'concept_path','T','LIKE','{concept_long}','{pref_label}'
				);\n""".format(**d)
        top_level_sql = top_level_sql.replace("\t", " ").replace("\n", " ")
        ## Combine multiple spaces to single space
        top_level_sql = " ".join(top_level_sql.split())
        return top_level_sql

    @classmethod
    def _concept_insert(cls, d, data_schema, concept_type_table, sourcesystem) -> str:
        """Return SQL line for single concept to data table"""
        d["data_schema"] = data_schema
        d["concept_type_table"] = concept_type_table
        d["sourcesystem"] = sourcesystem
        if d["notation"] == "":
            d["notation"] = "NULL"
        # logger.debug("Using dict for sql writing: {}".format(d))
        d['ct'] = "concept"
        if concept_type_table == "modifier_dimension":
            d['ct'] = "modifier"
        # logger.debug("Set d['ct'] to '{}' based on table_type: {}".format(d['ct'], concept_type_table))
        concept_sql = """INSERT INTO {data_schema}.{concept_type_table} (
				{ct}_path,{ct}_cd,name_char,update_date,
				download_date,import_date,sourcesystem_cd
			) VALUES(
				'{concept_long}','{notation}','{pref_label}',current_timestamp,
				'{fetch_timestamp}',current_timestamp,'{sourcesystem}'
			);\n""".format(**d)
        concept_sql = concept_sql.replace("\t", " ").replace("\n", " ")
        ## Combine multiple spaces to single space
        concept_sql = " ".join(concept_sql.split())
        return concept_sql

    @classmethod
    def _data_to_csv(cls, ordered_cols:list, d:dict, delim:str = ";") -> list:
        """Generate a csv line given the columns and data provided"""
        first = True
        line = []
        d["ontology_tablename"] = app.config["ontology_tablename"]
        for col_name in ordered_cols:
            new_value = ""
            if col_name in app.config["fixed_value_cols"]:
                ## Inject fixed values for some columns (see sql inserts)
                new_value = str(app.config["fixed_value_cols"].get(col_name, ""))
            elif col_name in app.config["sql_col_object_property_map"]:
                ## Lookup name map as sql cols differ from attribute/property names here
                real_property = str(app.config["sql_col_object_property_map"].get(col_name, ""))
                new_value = str(d.get(real_property, ""))
                logger.debug("mismatched sql({})/object({}) name for '{}', value: {}".format(col_name, real_property, d["pref_label"], new_value))
            else:
                new_value = str(d.get(col_name, ""))
                logger.debug("Matched and available col_name ({}) name for '{}', value: {}".format(col_name, d["pref_label"], new_value))
            if first:
                first = False
                line = [new_value]
            else:
                line.append(new_value)
        return line
    @classmethod
    def _data_to_csv_str(cls, ordered_cols:list, d:dict, delim:str = ";") -> str:
        """Generate a csv line given the columns and data provided"""
        first = True
        line = ""
        d["ontology_tablename"] = app.config["ontology_tablename"]
        for col_name in ordered_cols:
            new_value = ""
            if col_name in app.config["fixed_value_cols"]:
                ## Inject fixed values for some columns (see sql inserts)
                new_value = str(app.config["fixed_value_cols"].get(col_name, ""))
            elif col_name in app.config["sql_col_object_property_map"]:
                ## Lookup name map as sql cols differ from attribute/property names here
                real_property = str(app.config["sql_col_object_property_map"].get(col_name, ""))
                new_value = str(d.get(real_property, ""))
                logger.debug("mismatched sql({})/object({}) name for '{}', value: {}".format(col_name, real_property, d["pref_label"], new_value))
            else:
                new_value = str(d.get(col_name, ""))
                logger.debug("Matched and available col_name ({}) name for '{}', value: {}".format(col_name, d["pref_label"], new_value))
            if first:
                first = False
                line = new_value
            else:
                line += "{}{}".format(delim, new_value)
        return line

    def __dict__(self):
        """Return all properties which are useful as well as any regular attributes"""
        all_attributes = [
            "c_table_cd", "c_hlevel", "concept_long", "concept_long_hash8", "pref_label", "visual_attribute", "datatype_xml", "c_facttablecolumn", "c_tablename", "c_columnname",
            "description", "descriptions", "applied_path", "fetch_timestamp", "notation", "notations", "sourcesystem_cd"
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
        """Dynamically calculated - multi container only used if node has children"""
        # logger.debug("Checking parent node '{}' for element path stem: {}".format(self.containing_node, self.containing_node.element_path))
        sep = app.config["i2b2_path_separator"]
        impc = app.config["i2b2_multipath_container"]
        pnp = self.containing_node.element_path
        notation_path = ""
        if self.notation == "":
            notation_path = r"{pnp}{impc}{sep}".format(pnp = pnp, impc = impc, sep = sep)
        elif self.containing_node.child_nodes is None or len(self.containing_node.child_nodes) == 0:
            ## No multi-path hidden container when there are no child nodes
            notation_path = r"{pnp}{ni}{sep}".format(
                pnp = pnp,
                sep = sep,
                ni = list(self.containing_node.notations.values()).index(self)
                )
        else:
            notation_path = r"{pnp}{impc}{sep}{ni}{sep}".format(
                pnp = pnp,
                sep = sep,
                impc = impc,
                ni = list(self.containing_node.notations.values()).index(self)
                )
        # logger.debug("Calculated notation path for '{}': {}".format(self.notation, notation_path))
        return notation_path.replace("\\\\", "\\").replace("//", "/")
    @property
    def c_hlevel(self) -> int:
        """Dynamically calculated. \MULTI\ is +1, actual notations are +2"""
        if self.notation == "" or self.containing_node.child_nodes is None or len(self.containing_node.child_nodes) == 0:
            ## Hidden container \MULTI\
            ## Or when no children
            return self.containing_node.c_hlevel + 1
        else:
            return self.containing_node.c_hlevel + 2
    @property
    def notation(self) -> str:
        """Get the notation string for this level"""
        if self._notation is None:
            return ""
        else:
            return self._notation

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
        all_attributes = ["c_hlevel", "concept_long", "pref_label", "visual_attribute", "datatype_xml", "c_facttablecolumn", "c_tablename", "c_columnname", "description", "descriptions", "applied_path", "fetch_timestamp", "concept_long_hash8", "notation", "notations"]
        d1 = {k: getattr(self.containing_node, k, None) for k in all_attributes if not hasattr(self, k)}
        d2 = {k: getattr(self, k, None) for k in all_attributes if hasattr(self, k)}
        return {**d1, **d2}

##End
