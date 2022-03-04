""" meta_node.py
Object model for a metadata node
"""
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
    LONGSTRING = 3
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

    ## Used as title
    pref_labels:dict[str:str] = None
    ## Optionnaly displayed in CoMetaR top right
    alt_labels:dict[str:str] = None
    ## Used in sidebar/tree
    _display_labels:dict[str:str] = None
    ## Codes
    notations:dict[str:str] = None
    ## unit
    unit:str = None
    ## i2b2hidden
    dwh_display:str = None
    ## Main body text for node
    description:dict[str:str] = None
    node_type:Enum = None
    status:Enum = None
    data_type:Enum = None

    @property
    def top_level_node(self) -> bool:
        """ Dynamically calculated
        Dependant on parent not existing"""
        if self.parent_node is None:
            return False
        else:
            return True
    @property
    def ancestor_count(self) -> int:
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
                return self.parent.ancestor_count

        return ancestors

    @property
    def visual_attribute(self) -> str:
        """ Dynamically calculated based on node_type and dwh_display
        Coded display charachteristics for i2b2 tree"""
        if self.node_type is NodeType.collection:
            va_part1 = "C"
        elif len(self.children) > 0:
            if self.NodeType.MODIFIER:
                va_part1 = "D"
            else:
                va_part1 = "F"
        elif len(self.notations) <= 1:
            if self.NodeType.MODIFIER:
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
        if self.NodeType.MODIFIER:
            built_path = "\{}".format(self.name)
        elif self.top_level_node:
            built_path = "{}\{}".format(app.config["i2b2_path_prefix"], self.parent.element_path)
        else:
            built_path = "{}\{}".format(self.parent.element_path, self.name)
        return built_path
    @property
    def c_hlevel(self) -> int:
        """Dynamically calculated"""
        if self.NodeType.MODIFIER:
            return self.ancestor_count + 1
        else:
            return self.ancestor_count + 2

    @property
    def display_labels(self):
        """Default to pref_labels"""
        if self._display_labels and len(self._display_labels) > 0:
            return self._display_labels
        else:
            return self.pref_labels
    @display_labels.setter
    def display_labels(self, display_labels):
        """Simple"""
        self._display_labels = display_labels

    def __init__(self) -> None:
        """Initialise an instance with data"""

class NotationNode(object):
    """Sometimes we have multiple notations, each needs a node in i2b2 but is mostly inherited from the parent concept"""

    @property
    def visual_attribute(self) -> str:
        """For this niche category, its always "LH" (a hidden leaf)"""
        return "LH"
    @property
    def element_path(self) -> str:
        """Dynamically calculated"""
        notation_path = self.parent_node.element_path + "/MULTI/" + self.parent_node.notations.index(self)
        app.logger.debug("Calculated notation path for '{}': {}".format(self.name, notation_path))
        return notation_path
    @property
    def c_hlevel(self) -> int:
        """Dynamically calculated /MULTI/ is +1, actual notations are +2"""
        return self.parent_node.c_hlevel + 2

    def __init__(self, parent_node) -> None:
        """We want to know which instance is our parent, then we can extend it's attributes"""
        self.parent_node = parent_node
        # super().__init__()

##End
