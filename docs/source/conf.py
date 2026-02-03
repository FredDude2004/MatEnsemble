from __future__ import annotations

import sys
from pathlib import Path

# Make project importable regardless of whether conf.py lives in docs/ or docs/source/
HERE = Path(__file__).resolve()
for parent in HERE.parents:
    if (parent / "matensemble").is_dir():
        sys.path.insert(0, str(parent))
        break

project = "MatEnsemble"
author = "FredDude2004"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns: list[str] = []

# If you don't have Flux bindings installed locally, mock them so autodoc can import.
autodoc_mock_imports = [
    "flux",
    "flux.job",
    "mpi4py",
    "redis",
    "pandas",
]

autosummary_generate = True
root_doc = "index"

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
