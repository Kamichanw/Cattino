# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import inspect
import logging
import os
import sys
import requests

from sphinx.ext import autodoc

sys.path.append(os.path.abspath("../.."))
logger = logging.getLogger(__name__)

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Cattino"
copyright = "2025, Kamichanw"
author = "Kamichanw"
release = "0.1.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.napoleon",
    "sphinx.ext.linkcode",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "myst_parser",
    "sphinxarg.ext",
    "sphinx_design",
    "sphinx_togglebutton",
]
myst_enable_extensions = [
    "colon_fence",
]

templates_path = ["_templates"]
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"
html_static_path = ["_static"]
html_js_files = ["custom.js"]
html_css_files = ["custom.css"]
html_theme_options = {
    "path_to_docs": "docs/source",
    "repository_url": "https://github.com/Kamichanw/Cattino",
    "use_repository_button": True,
    "use_edit_page_button": True,
    "use_issues_button": True,
}

def get_repo_base_and_branch(pr_number):
    global _cached_base, _cached_branch
    if _cached_base and _cached_branch:
        return _cached_base, _cached_branch

    url = f"https://api.github.com/repos/Kamichanw/Cattino/pulls/{pr_number}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        _cached_base = data['head']['repo']['full_name']
        _cached_branch = data['head']['ref']
        return _cached_base, _cached_branch
    else:
        logger.error("Failed to fetch PR details: %s", response)
        return None, None
    
def linkcode_resolve(domain, info):
    if domain != 'py':
        return None
    if not info['module']:
        return None
    filename = info['module'].replace('.', '/')
    module = info['module']

    # try to determine the correct file and line number to link to
    obj = sys.modules[module]

    # get as specific as we can
    lineno: int = 0
    filename: str = ""
    try:
        for part in info['fullname'].split('.'):
            obj = getattr(obj, part)

            if not (inspect.isclass(obj) or inspect.isfunction(obj)
                    or inspect.ismethod(obj)):
                obj = obj.__class__  # Get the class of the instance

            lineno = inspect.getsourcelines(obj)[1]
            filename = (inspect.getsourcefile(obj)
                        or f"{filename}.py").split("vllm/", 1)[1]
    except Exception:
        # For some things, like a class member, won't work, so
        # we'll use the line number of the parent (the class)
        pass

    if filename.startswith("checkouts/"):
        # a PR build on readthedocs
        pr_number = filename.split("/")[1]
        filename = filename.split("/", 2)[2]
        base, branch = get_repo_base_and_branch(pr_number)
        if base and branch:
            return f"https://github.com/{base}/blob/{branch}/{filename}#L{lineno}"

    # Otherwise, link to the source file on the main branch
    return f"https://github.com/Kamichanw/Cattino/blob/main/{filename}#L{lineno}"

class MockedClassDocumenter(autodoc.ClassDocumenter):
    """Remove note about base class when a class is derived from object."""

    def add_line(self, line: str, source: str, *lineno: int) -> None:
        if line == "   Bases: :py:class:`object`":
            return
        super().add_line(line, source, *lineno)


autodoc.ClassDocumenter = MockedClassDocumenter

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "typing_extensions":
    ("https://typing-extensions.readthedocs.io/en/latest", None),
    "aiohttp": ("https://docs.aiohttp.org/en/stable", None),
    "pillow": ("https://pillow.readthedocs.io/en/stable", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "torch": ("https://pytorch.org/docs/stable", None),
    "psutil": ("https://psutil.readthedocs.io/en/stable", None),
}

autodoc_preserve_defaults = True