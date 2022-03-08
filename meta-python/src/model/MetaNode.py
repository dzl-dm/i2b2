""" meta_node.py
Object model for a metadata node
"""
import datetime
from enum import Enum
from flask import current_app as app

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

    ## unit
    unit:str = None
    ## i2b2hidden
    dwh_display:str = None
    ## Where a modifier is applicable
    _applied_path:str = None
    ## When the object is created
    fetch_time:str = None
    _node_type:NodeType = None
    status:NodeStatus = None
    datatype_raw:NodeDatatype = None

    @property
    def top_level_node(self) -> bool:
        """ Dynamically calculated
        Dependant on parent not existing"""
        if self.parent_node is None:
            app.logger.info("Node '{}' is a top level element, it doesn't have any parents".format(self.name))
            return True
        else:
            return False
    @property
    def ancestor_count_ORIG(self) -> int:
        """ Dynamically calculated
        Dependant on parent (eventually) not existing"""
        ## TODO: Is it instance safe?
        ## Otherwise count number of slashes in element_path?
        counter = 0
        def ancestors(n):

            nonlocal counter
            counter += 1
            ancestors.counter = counter

            if self.parent_node is None:
                return counter
            else:
                return self.parent_node.ancestor_count

        return ancestors(counter)
    @property
    def ancestor_count(self) -> int:
        """ Dynamically calculated
        Dependant on parent (eventually) not existing"""
        if self.parent_node is None:
            return 0
        else:
            return self.parent_node.ancestor_count + 1

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
        if self.node_type == NodeType.MODIFIER:
            built_path = "{sep}{np}{sep}".format(np = self.name, sep = sep)
        elif self.top_level_node:
            built_path = "{sep}{ipp}{sep}{np}{sep}".format(ipp = ipp, np = self.name, sep = sep)
        else:
            built_path = "{pnp}{sep}{np}{sep}".format(pnp = self.parent_node.element_path, np = self.name, sep = sep)
        return built_path
    @property
    def c_hlevel(self) -> int:
        """Dynamically calculated"""
        if self.node_type == NodeType.MODIFIER:
            return self.ancestor_count + 1
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
            app.logger.debug("All keys for '{}': {}".format(self.node_uri, self.pref_labels.keys()))
            single_label = self.pref_labels.keys()[0]
        return single_label
    @property
    def datatype(self) -> NodeDatatype:
        """The raw Enum representation of the datatype"""
        return self.datatype_raw
    @datatype.setter
    def datatype(self, dtype:str):
        """Convert incoming string based indication to enum"""
        if dtype is not None and dtype != "":
            types = {NodeDatatype.INTEGER: ["int", "integer"], NodeDatatype.FLOAT: ["float", "dec", "decimal"], NodeDatatype.STRING: ["string", "str"], NodeDatatype.LARGESTRING: ["largestring"]}
            incoming_types = {v:k for k, l in types.items() for v in l}
            self.datatype_raw = incoming_types.get[dtype.lower(), None]
    @property
    def datatype_pretty(self) -> str:
        """ String version of datatype. With correct case for i2b2 """
        pretty_dt = None
        if self.datatype_raw is not None:
            dt_prettify = {NodeDatatype.INTEGER: "Integer", NodeDatatype.FLOAT: "Float",NodeDatatype.STRING: "String", NodeDatatype.LARGESTRING: "largeString"}
            pretty_dt = dt_prettify[self.datatype_raw]
        return pretty_dt
    @property
    def datatype_xml(self) -> str:
        """ XML string for datatype """
        xml_dt = "NULL"
        if self.datatype_pretty is not None:
            xml_dt = "'<ValueMetadata><Version>3.02</Version><CreationDateTime>{fetch_time}</CreationDateTime><DataType>{datatype_pretty}</DataType><Oktousevalues>Y</Oktousevalues></ValueMetadata>'".format(
                fetch_time=self.fetch_time, datatype_pretty=self.datatype_pretty
                )
        return xml_dt

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
        if self._applied_path is None:
            # if self.node_type.MODIFIER:
            if self.node_type == NodeType.MODIFIER:
                app.logger.warn("Modifier node ({}) should have 'applied_path'!".format(self.pref_labels))
            return "@"
        else:
            return self._applied_path
    @applied_path.setter
    def applied_path(self, path:str):
        """Path where the modifier is applicable"""
        self._applied_path = path

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
        return self._notations
    @property
    def notation(self) -> str:
        """Return the notation for the base Node (only a real notation if there exists only 1 notation, otherwise its empty)"""
        if len(self.notations) == 1:
            return self.notations.keys()[0]
        else:
            return ""
    @notations.setter
    def notations(self, notations):
        """Create objects from name(s)"""
        if notations is None or len(notations) == 0:
            ## No notations
            self._notations = None
        elif len(notations) == 1:
            ## Simple notations
            self._notations = notations
        else:
            ## Multi notations
            new_notations = {}
            new_notations[app.config["i2b2_multipath_container"]] = NotationNode(self, None)
            for notation, tag in notations.items():
                new_notations[notation] = NotationNode(self, notation, tag)
            self._notations = new_notations

    @property
    def node_type(self) -> dict:
        """Return node_type enum"""
        return self._node_type
    @node_type.setter
    def node_type(self, node_type:str):
        """Set node_type enum NodeType"""
        if node_type is None:
            app.logger.error("Node ({}) must have a type, not None!".format(self.name))
        elif node_type.lower() in ["concept"]:
            self._node_type = NodeType.CONCEPT
        elif node_type.lower() in ["modifier"]:
            self._node_type = NodeType.MODIFIER
        elif node_type.lower() in ["collection"]:
            self._node_type = NodeType.COLLECTION
        else:
            app.logger.error("Node ({}) must have a type! {}".format(self.name, node_type))

    @property
    def meta_inserts(self) -> list:
        """SQL inserts for the i2b2metadata i2b2 (and table_access dependant on need)"""
        d = self._attribute_like()
        # app.logger.debug("computed members: {}".format(d))

        ## meta_inserts' dict can have 2 entries, ontology and (optionally) table_access
        inserts = {}
        ## ontology can have multiple entries when there
        inserts["ontology"] = []
        ## Always insert the base node
        inserts["ontology"].append(MetaNode._single_ontology_insert(d = d, meta_schema = app.config["meta_schema"], ontology_tablename = app.config["ontology_tablename"], sourcesystem = app.config["sourcesystem"]))
        if self.notations and len(self.notations) >= 2:
            ## If multiple notations, use NotationNode objects to populate the additional INSERT's
            for notation_obj in self.notations.values():
                inserts["ontology"].append(MetaNode._single_ontology_insert(d = notation_obj._attribute_like(), meta_schema = app.config["meta_schema"], ontology_tablename = app.config["ontology_tablename"], sourcesystem = app.config["sourcesystem"]))

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
        d = self._attribute_like()
        # app.logger.debug("computed members: {}".format(d))

        if self.node_type == NodeType.CONCEPT:
            concept_type_table = "concept_dimension"
        elif self.node_type == NodeType.MODIFIER:
            concept_type_table = "modifier_dimension"
        else:
            app.logger.error("This node ({}) should be a concept or modifier, its niether! {}".format(self.node_uri, self.node_type))
            return None
        ## data inserts occur once for each notation, but not for the containing concept (unless its a single notation)
        inserts = []
        if len(self.notations) == 1:
            inserts.append(MetaNode._concept_insert(d = d, data_schema = app.config["data_schema"], concept_type_table = concept_type_table, sourcesystem = app.config["sourcesystem"]))
        else:
            for notation_obj in self.notations.values():
                if notation_obj.notation is not None and notation_obj.notation != "":
                    inserts["ontology"].append(MetaNode._single_ontology_insert(d = notation_obj._attribute_like(), data_schema = app.config["data_schema"], concept_type_table = concept_type_table, sourcesystem = app.config["sourcesystem"]))
        return inserts

    def __init__(self, node_uri, name, node_type, pref_labels, display_labels, notations, descriptions, alt_labels = None, datatype = None, display_status = None) -> None:
        """Initialise an instance with data"""
        ## Set fetch_time
        self.node_uri = node_uri
        self.name = name
        self.node_type = node_type
        self.pref_labels = pref_labels
        self.display_labels = display_labels
        self.notations = notations
        self.descriptions = descriptions
        self.alt_labels = alt_labels
        self.datatype = datatype
        self.display_status = display_status
        ## TODO: take time format from config
        self.fetch_time = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        app.logger.info("New MetaNode object created! ({})".format(self.node_uri))

    def _attribute_like(self) -> dict:
        """Return dict of all attributes and properties of this object"""
        ## NOTE: Dynamically checking all attributes requires eveluating them to check the type, so that will cause infinite loops...
        # import time
        # # app.logger.debug("MetaNode __dir__ ({}): {}".format(len(self.__dir__()), self.__dir__()))
        # # app.logger.debug("{}: {}".format("_single_ontology_insert", getattr(o, "_single_ontology_insert", '')))
        # for k in ["top_level_node", "__init__", "_single_ontology_insert", "node_type", "node_uri", "data_inserts", "meta_inserts"]:
        #     # app.logger.debug("{}: {}".format(k, hasattr(self, k, "")))
        #     app.logger.debug("{}: {}".format(k, type(self[k]).__name__))
        # time.sleep(50)

        # d = {k: getattr(self, k, '') for k in self.__dir__() if k[:2] != '__' and type(getattr(self, k, '')).__name__ != 'method'}
        # app.logger.debug("MetaNode d ({}): {}".format(len(d), d))

        attributes = ["c_hlevel", "concept_long", "pref_label", "visual_attribute", "datatypexml", "c_facttablecolumn", "c_tablename", "c_columnname", "description", "applied_path", "fetch_timestamp", "concept_long_hash8", "notation"]
        d = {k: getattr(self, k, None) for k in attributes}
        # app.logger.debug("Created attribute dict for sql writing: {}".format(d))
        return d

    @classmethod
    def _single_ontology_insert(cls, d, meta_schema, ontology_tablename, sourcesystem) -> str:
        """Return SQL line for single notation insert to ontology table"""
        d["meta_schema"] = meta_schema
        d["ontology_tablename"] = ontology_tablename
        d["sourcesystem"] = sourcesystem
        app.logger.debug("Using dict for sql writing: {}".format(d))
        notation_sql = """INSERT INTO {meta_schema}.{ontology_tablename} (
            c_hlevel,c_fullname,c_name,c_synonym_cd,c_visualattributes,
            c_basecode,c_metadataxml,c_facttablecolumn,c_tablename,c_columnname,
            c_columndatatype,c_operator,c_dimcode,c_tooltip,m_applied_path,
            update_date,download_date,import_date,sourcesystem_cd)
        VALUES(
            {c_hlevel},'{concept_long}','{pref_label}','N','{visual_attribute}',
            {notation},{datatypexml},'{c_facttablecolumn}','{c_tablename}','{c_columnname}',
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
        app.logger.debug("Using dict for sql writing: {}".format(d))
        top_level_sql = """INSERT INTO {meta_schema}.table_access (
					c_table_cd,c_table_name,c_protected_access,c_hlevel,c_fullname,
					c_name,c_synonym_cd,c_visualattributes,c_facttablecolumn,c_dimtablename,
					c_columnname,c_columndatatype,c_operator,c_dimcode,c_tooltip
				) VALUES(
					'i2b2_{concept_long_hash8}','{ontology_tablename}','N',1,'{concept_long}',
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
        concept_sql = """INSERT INTO {data_schema}.{concept_type_table}
				concept_path,concept_cd,name_char,update_date,
				download_date,import_date,sourcesystem_cd
			) VALUES(
				'{concept_long}','{notation}','{pref_label}',current_timestamp,
				'{fetch_timestamp}',current_timestamp,'{sourcesystem}'
			);\n""".format(d = d, data_schema = data_schema, concept_type_table = concept_type_table, sourcesystem = sourcesystem)
        concept_sql = concept_sql.replace("\t", " ").replace("\n", " ")
        ## Combine multiple spaces to single space
        concept_sql = " ".join(concept_sql.split())
        return concept_sql


class NotationNode(object):
    """Sometimes we have multiple notations, each needs a node in i2b2 but is mostly inherited from the parent concept"""

    @property
    def visual_attribute(self) -> str:
        """For this niche category, its always "LH" (a hidden leaf)"""
        return "LH"
    @property
    def element_path(self) -> str:
        """Dynamically calculated"""
        app.logger.debug("Checking parent node '{}' for element path stem: {}".format(self.parent_node, self.parent_node.element_path))
        # app.logger.debug("Using notation '{}' to build path".format(self.notation))
        # app.logger.debug("Parent notations: {}".format(self.parent_node.notations.values()))
        # app.logger.debug("self: {}".format(self))
        # app.logger.debug("Notation index: {}".format(list(self.parent_node.notations.values()).index(self)))
        sep = app.config["i2b2_path_separator"]
        impc = app.config["i2b2_multipath_container"]
        notation_path = ""
        if self.notation == "":
            notation_path = "{pnp}{impc}{sep}".format(pnp = self.parent_node.element_path, impc = impc, sep = sep)
        else:
            notation_path = "{pnp}{impc}{sep}{ni}{sep}".format(
                pnp = self.parent_node.element_path,
                sep = sep,
                impc = impc,
                ni = list(self.parent_node.notations.values()).index(self)
                )
        # notation_path = self.parent_node.element_path + "\\MULTI\\" + self.parent_node.notations.index(self)
        app.logger.debug("Calculated notation path for '{}': {}".format(self.notation, notation_path))
        return notation_path
    @property
    def c_hlevel(self) -> int:
        """Dynamically calculated \MULTI\ is +1, actual notations are +2"""
        if self.notation == "":
            ## Hidden container \MULTI\
            return self.parent_node.c_hlevel + 1
        else:
            return self.parent_node.c_hlevel + 2
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
        app.logger.debug("NOTATION! Getting concept_long/element_path '{}' for: {}".format(self.element_path, self.notation))
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

    def __init__(self, parent_node, notation = None, tag = None) -> None:
        """We want to know which instance is our parent, then we can extend it's attributes"""
        self.parent_node = parent_node
        self._notation = notation
        self.tag = tag

    def _attribute_like(self) -> dict:
        """Return dict of all attributes and properties of this object"""
        parent_attributes = ["pref_label", "datatypexml", "c_facttablecolumn", "c_tablename", "c_columnname", "description", "applied_path", "fetch_timestamp"]
        notation_attributes = ["c_hlevel", "visual_attribute", "concept_long", "concept_long_hash8", "notation"]
        d1 = {k: getattr(self.parent_node, k, None) for k in parent_attributes}
        d2 = {k: getattr(self, k, None) for k in notation_attributes}
        # app.logger.debug("Created attribute dict for sql writing: {}".format({**d1, **d2}))
        return {**d1, **d2}

##End
