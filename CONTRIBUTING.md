# Contributing

Contributions to `dataretrieval` are welcome and greatly appreciated, but please
read this document for information on *how* to contribute.

`dataretrieval` broadly follows a ["forking" workflow](https://docs.github.com/en/get-started/quickstart/contributing-to-projects),
however writing code is not the only way to contribute.

## Ways to contribute

### Reporting Bugs

Report bugs at https://github.com/thodson-usgs/dataretrieval/issues.

When reporting a bug, please include:

* Detailed steps to reproduce the bug
* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.

### Fixing Bugs

Look through the GitHub issues for bugs. Anything tagged as a "bug" is open to
whomever wants to fix it.


### Implementing Features

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "please-help" is open to whomever wants to implement it.

Please do not combine multiple feature enhancements into a single pull request.


### Writing Documentation

dataretrieval could always use more documentation, whether as part of the
official docs, in docstrings, or even in blog posts or articles.


### Submiting Feedback

The best way to send feedback is to file an issue at
https://github.com/thodson-usgs/dataretrieval/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.


# Contributor Guidelines


## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. Any pull request should include tests. However, a contribution with
   no tests is preferablble to no contribution at all.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.md.
3. The pull request should work for Python 3.4, 3.5, 3.6, and PyPy on
   Appveyor and Travis CI.


## Coding Standards

* PEP8
* Docstrings should follow the numpy standard:
  * Example:
    ``` python
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

    >>> foo(1,'bar')
    True

    """
    ```
  * For more details see https://github.com/sphinx-doc/sphinx/blob/master/doc/ext/example_numpy.py
* The public interace should emphasize functions over classes; however, classes can and should be used internally and in tests.
* Functions for downloading data from a specific web portal must be grouped within their own submodule.
  * For example, all NWIS functions are located at `dataretrieval.nwis`.

* Quotes via http://stackoverflow.com/a/56190/5549

  * Use double quotes around strings that are used for interpolation or that are natural language messages
  * Use single quotes for small symbol-like strings (but break the rules if the strings contain quotes)
  * Use triple double quotes for docstrings and raw string literals for regular expressions even if they aren't needed.
  * Example:

    ``` python

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
    ```

  * Write new code in Python 3.

Acknowledgements
================
This document was adapted from the cookiecutter project's [CONTRIBUTING file](https://github.com/audreyr/cookiecutter/blob/master/CONTRIBUTING.rst).
