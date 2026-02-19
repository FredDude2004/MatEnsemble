# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys

sys.path.insert(0, os.path.abspath("../../src"))

project = "MatEnsemble"
copyright = "2026, Soumendu Bagchi, Kaleb Duchesneau"
author = "Soumendu Bagchi, Kaleb Duchesneau"
release = "v0.1.2"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
    "myst_parser",
]

autosummary_generate = True

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "private-members": False,  # set True if you want _private too
    "special-members": "__init__",  # include constructors
    "show-inheritance": True,
    "inherited-members": True,
    "member-order": "bysource",
}

# Optional but helpful:
napoleon_google_docstring = True
napoleon_numpy_docstring = True

templates_path = ["_templates"]
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
autosummary_generate = True

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
    "inherited-members": True,
    "member-order": "bysource",
}

autodoc_typehints = "description"
napoleon_google_docstring = True
napoleon_numpy_docstring = True

# autodoc_mock_imports = [
#     "flux",
#     "flux.job",
#     "mpi4py",
#     "mpi4py.MPI",
#     "lammps",
#     "numpy",
#     "pandas",
#     "torch",
#     "scipy",
#     "sklearn",
#     "matplotlib",
#     "matplotlib.pyplot",
#     "ase",
#     "pymatgen",
#     "seaborn",
#     "ovito",
#     "ovito.io",
#     "ovito.data",
#     "ovito.pipeline",
#     "redis",
# ]
