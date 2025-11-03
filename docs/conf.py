# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
sys.path.insert(0, os.path.abspath('..'))

project = 'planets_delaer_pipeline'
copyright = '2025, Hozan'
author = 'Hozan'
release = '1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# === ÄNDERUNG HIER ===
# Füge 'autodoc' (für Code-Import) und 'sphinx_design' (für Tabs) hinzu
extensions = [
    'sphinx.ext.autodoc',
    'sphinx_design',  # <- Stelle sicher, dass DIESER Name hier steht
]

# ... und stelle sicher, dass das Theme gesetzt ist:
# === ENDE ÄNDERUNG ===

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# === ÄNDERUNG HIER (Optional, aber empfohlen) ===
# Wechsle zu einem moderneren Theme
#html_theme = 'sphinx_rtd_theme'
html_theme = 'furo'
# oder 'sphinx_rtd_theme' (nach 'pip install sphinx-rtd-theme')
# html_theme = 'sphinx_rtd_theme'
# === ENDE ÄNDERUNG ===

html_static_path = ['_static', 'images']
html_logo = 'images/coders.png'