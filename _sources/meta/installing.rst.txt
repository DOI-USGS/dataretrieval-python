Installation Guide
==================

Whether you are a user or developer we recommend installing ``dataretrieval``
in a virtual environment. This can be done using something like ``virtualenv``
or ``conda``. Package dependencies are listed in the `requirements.txt`_ file,
a full list of dependencies necessary for development are listed in the
`requirements-dev.txt`_ file.

.. _requirements.txt: https://github.com/DOI-USGS/dataretrieval-python/blob/master/requirements.txt

.. _requirements-dev.txt: https://github.com/DOI-USGS/dataretrieval-python/blob/master/requirements-dev.txt


User Installation
-----------------

Via ``pip``:
^^^^^^^^^^^^
To install the latest stable release of ``dataretrieval`` from `PyPI`_, run the
following commands:

.. code-block:: bash

    $ pip install dataretrieval

.. _PyPI: https://pypi.org/project/dataretrieval


Via ``conda``:
^^^^^^^^^^^^^^
To install the latest stable release of ``dataretrieval`` from the
`conda-forge channel`_, run the following commands:

.. code-block:: bash

    $ conda -c conda-forge install dataretrieval

.. _conda-forge channel: https://anaconda.org/conda-forge/dataretrieval


Developer Installation
----------------------

To install ``dataretrieval`` for development, we recommend first forking
the repository on GitHub. This will allow you to develop on your own
feature branch, and propose changes as pull requests to the main branch of
the repository.

The first step is to clone your fork of the repository:

.. code-block:: bash

    $ git clone https://github.com/DOI-USGS/dataretrieval-python.git

Then, set the cloned repository as your current working directory in your
terminal and run the following commands to get an "editable" installation of
the package for development:

.. code-block:: bash

    $ pip install -r requirements-dev.txt
    $ pip install -e .

To check your installation you can run the tests with the following commands:

.. code-block:: bash

    $ cd tests
    $ pytest

In order to fetch the latest version of ``dataretrieval``, we recommend
defining the main repository as a remote `upstream` repository:

.. code-block:: bash

    $ git remote add upstream https://github.com/DOI-USGS/dataretrieval-python.git

You can also build the documentation locally by running the following commands:

.. code-block:: bash

    $ cd docs
    $ make docs

This both tests the documentation (runs code blocks and checks links), and also
locally *builds* the documentation, placing the HTML files within the
``docs/build/html`` directory. You can then open the ``index.html`` file in
your browser to view the documentation.