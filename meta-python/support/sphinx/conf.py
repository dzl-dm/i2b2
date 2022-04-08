import os
import sys

## Prerequisite - pip3 install sphinx sphinx-markdown-builder sphinx-autodoc-typehints
## This can be run with:
# sphinx-build -b markdown . _build

sys.path.append('/src/')
# enable autodoc to load local modules
sys.path.insert(0, os.path.abspath("."))

project = "listener"
copyright = "2022, DZL"
author = "DZL"
extensions = ["sphinx.ext.autodoc", "sphinx.ext.intersphinx", "sphinx_autodoc_typehints"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
templates_path = ["_templates"]
html_theme = "alabaster"
html_static_path = ["_static"]
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None)
}
html_theme_options = {"nosidebar": True}
autodoc_typehints = "description"
