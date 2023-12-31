import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join("..", "..")))

project = "dcraft"
copyright = "2023, Shuhei Kishi"
author = "Shuhei Kishi"
release = "0.0.2"


extensions = ["sphinx.ext.autodoc", "sphinx.ext.napoleon", "sphinx_rtd_theme"]


templates_path = ["_templates"]
exclude_patterns = []


html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
