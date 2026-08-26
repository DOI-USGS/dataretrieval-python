Installation Guide
==================

Whether you are a user or a developer, we recommend installing ``dataretrieval``
in a virtual environment, using a tool such as ``virtualenv`` or ``conda``.
Package dependencies are declared in ``pyproject.toml``: the core runtime
dependencies under ``[project.dependencies]``, and optional extras (``test``,
``doc``, ``nldi``) under ``[project.optional-dependencies]``.


User Installation
-----------------

Via ``pip``:
^^^^^^^^^^^^
To install the latest stable release of ``dataretrieval`` from `PyPI`_, run the
following command:

.. code-block:: bash

    $ pip install dataretrieval

.. _PyPI: https://pypi.org/project/dataretrieval


Via ``conda``:
^^^^^^^^^^^^^^
To install the latest stable release of ``dataretrieval`` from the
`conda-forge channel`_, run the following command:

.. code-block:: bash

    $ conda install -c conda-forge dataretrieval

.. _conda-forge channel: https://anaconda.org/conda-forge/dataretrieval


Developer Installation
----------------------

To install ``dataretrieval`` for development, we recommend first forking
the repository on GitHub. Forking lets you develop on your own feature
branch and propose changes as pull requests to the main branch of
the repository.

First, clone your fork of the repository:

.. code-block:: bash

    $ git clone https://github.com/DOI-USGS/dataretrieval-python.git

Then, make the cloned repository your working directory and run the following
command to get an "editable" installation of the package for development:

.. code-block:: bash

    $ pip install -e ".[test,doc,nldi]"

This installs ``dataretrieval`` in editable mode along with the development
extras: ``test`` (test runner), ``doc`` (documentation build), and ``nldi``
(``geopandas``, required by the NLDI module).

To check your installation, run the tests with the following commands:

.. code-block:: bash

    $ cd tests
    $ pytest

To fetch the latest version of ``dataretrieval``, we recommend
defining the main repository as a remote `upstream` repository:

.. code-block:: bash

    $ git remote add upstream https://github.com/DOI-USGS/dataretrieval-python.git

You can also build the documentation locally by running the following commands:

.. code-block:: bash

    $ cd docs
    $ make docs

These commands both test the documentation (running code blocks and checking
links) and *build* it locally, placing the HTML files within the
``docs/build/html`` directory. Open the ``index.html`` file in your browser to
view the documentation.
