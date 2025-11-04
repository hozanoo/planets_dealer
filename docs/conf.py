# Configuration file for the Sphinx documentation builder.
# Vollständige Doku: https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys
sys.path.insert(0, os.path.abspath('..'))

# -- Projektinformationen ----------------------------------------------------
project = 'planets_dealer_pipeline'
author = 'Hozan'
copyright = '2025, Hozan'
release = '1.0'
language = 'de'

# -- Erweiterungen -----------------------------------------------------------
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx_design',
]

autosectionlabel_prefix_document = True
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', {}),
}

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Nummerierung für Abbildungen, Tabellen usw. -----------------------------
numfig = True
numfig_format = {
    'figure': 'Abbildung %s',
    'table': 'Tabelle %s',
    'code-block': 'Listing %s',
    'section': 'Abschnitt %s',
}

# -- HTML-Output -------------------------------------------------------------
html_theme = 'pydata_sphinx_theme'

# Optionen für das PyData Theme
html_theme_options = {
    "logo": {
        "image_light": "images/coders_light.png",
        "image_dark": "images/coders.png",
    },
    "navbar_align": "content",       # zentriert oder "right"
    "navbar_end": ["theme-switcher", "icon-links"],  # Dark/Light Toggle
    "secondary_sidebar_items": ["page-toc"],         # Inhaltsverzeichnis rechts
    "use_edit_page_button": False,
    "show_nav_level": 2,             # Tiefe im Menübaum
    "navigation_with_keys": True,    # j/k für Seiten-Navigation
    "collapse_navigation": False,
}

# -- Logos und statische Dateien --------------------------------------------
html_logo = "images/coders.png"
html_static_path = ['_static', 'images']

# Optional: Farbschema (hell/dunkel)
pygments_style = 'sphinx'
pygments_dark_style = 'monokai'

# Optional: Anpassbare Fußzeile oder Social-Links
html_theme_options["icon_links"] = [
    {
        "name": "GitHub",
        "url": "https://github.com/hozanoo/planets_dealer",
        "icon": "fa-brands fa-github",
    },
]
