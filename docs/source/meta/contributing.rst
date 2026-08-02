Contributing
============

Contributions to ``dataretrieval`` are welcome and greatly appreciated, but
please read this document before doing so.


Ways to contribute
------------------

Reporting Bugs:
^^^^^^^^^^^^^^^

Report bugs at https://github.com/DOI-USGS/dataretrieval-python/issues

When reporting a bug, please include:

    - Detailed steps to reproduce the bug
    - Your operating system name and version.
    - Any details about your local setup that might be helpful in troubleshooting.

Fixing Bugs:
^^^^^^^^^^^^

Look through the GitHub issues for bugs. Anything tagged as a "bug" is open to
whomever wants to fix it.


Implementing Features:
^^^^^^^^^^^^^^^^^^^^^^

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "please-help" is open to whomever wants to implement it.

Please do not combine multiple feature enhancements into a single pull request.

Writing Documentation:
^^^^^^^^^^^^^^^^^^^^^^

``dataretrieval`` could always use more documentation, whether as part of the
official docs, in docstrings, or even in blog posts or articles.

Submitting Feedback:
^^^^^^^^^^^^^^^^^^^^

The best way to send feedback is to file an issue at
https://github.com/DOI-USGS/dataretrieval-python/issues

If you are proposing a feature:

    - Explain in detail how it would work.
    - Keep the scope as narrow as possible, to make it easier to implement.

Contributor Guidelines
----------------------

Pull Request Guidelines:
^^^^^^^^^^^^^^^^^^^^^^^^

Before you submit a pull request, check that it meets these guidelines:

1. Changes to package functionality should include tests.
2. Changes to behavior should update the relevant documentation or docstrings.
3. The pull request should work for Python 3.10 and later and pass the GitHub
   Actions continuous integration pipelines.
4. Build-related changes should preserve the installed-wheel smoke test; tests
   run from a source checkout do not prove that an artifact is complete.
5. Architecturally significant changes should update :doc:`../architecture/index`,
   add or supersede an ADR, and adjust the corresponding fitness function.


Updating Package Version:
^^^^^^^^^^^^^^^^^^^^^^^^^^

Follow semantic versioning as best as possible. This means that changing the
first digit of the version indicates a breaking change. Any smaller changes
should attempt to maintain backwards-compatibility with previous code and
issue deprecation warnings for features or functionality that will be removed
or no longer be backwards-compatible in future releases.

The package version is derived automatically from Git tags by
``setuptools_scm`` (see ``[tool.setuptools_scm]`` in ``pyproject.toml``), so
there is no version string to edit by hand. To cut a release, tag the commit
(for example, ``git tag v1.2.3``) and push the tag; both the installed package
version and the documentation's ``version`` and ``release`` values (read via
``importlib.metadata`` in ``docs/source/conf.py``) follow from it.


Coding Standards
----------------

Formatting, linting, and strict type checking are enforced by CI and the
pre-commit configuration. Run ``ruff check .``, ``ruff format --check .``,
``mypy``, and the relevant pytest suite before submitting a change.

    - Follow PEP8 (https://peps.python.org/pep-0008/).
    - Docstrings should follow the NumPy standard (`example`_).
    - The public interface should emphasize functions over classes; classes can
      and should be used internally and in tests.
    - Group public download functions by data portal.
    - Preserve the dependency direction in :doc:`../architecture/index`:
      public facades depend on service/protocol adapters, which depend on stable
      shared policy and infrastructure. Shared OGC code must not import service
      adapters, and modern modules must not depend on legacy NWIS.
    - Treat underscore-prefixed helpers as implementation details. Existing
      cross-package uses are documented variances, not extension points.

.. _example: https://www.sphinx-doc.org/en/master/usage/extensions/example_numpy.html

    - Example:

        .. code:: python

            def foo(param1, param2):
            """Example function with types documented in the docstring.

            A more detailed description of the function and its implementation.

            Parameters
            ----------
            param1 : int
                The first parameter.
            param2 : str
                The second parameter.

            Returns
            -------
            bool
                True if successful, False otherwise.

            Examples
            --------
            Examples should be written in doctest format and should demonstrate basic usage.

            .. doctest::

                >>> type(1) is int
                True

            """

    - The public interface should emphasize functions over classes; however, classes can and should be used internally and in tests.
    - Functions for downloading data from a specific web portal must be grouped within their own submodule.
    - For example, all NWIS functions are located at :obj:`dataretrieval.nwis`.

    - Quotes via http://stackoverflow.com/a/56190/5549:

    - Use double quotes around strings that are used for interpolation or that are natural language messages
    - Use single quotes for small symbol-like strings (but break the rules if the strings contain quotes)
    - Use triple double quotes for doc-strings and raw string literals for regular expressions even if they aren't needed.

    - Example:

    .. code:: python

        LIGHT_MESSAGES = {
            'English': "There are %(number_of_lights)s lights.",
            'Pirate':  "Arr! Thar be %(number_of_lights)s lights."
        }

        def lights_message(language, number_of_lights):
            """Return a language-appropriate string reporting the light count."""
            return LIGHT_MESSAGES[language] % locals()

        def is_pirate(message):
            """Return True if the given message sounds piratical."""
            return re.search(r"(?i)(arr|avast|yohoho)!", message) is not None


Acknowledgements
----------------
This document was adapted from the ``cookiecutter`` project's CONTRIBUTING file, which resides at
https://github.com/cookiecutter/cookiecutter/blob/main/CONTRIBUTING.md
Thank you to the ``cookiecutter`` team for helping streamline open-source development for the masses.
