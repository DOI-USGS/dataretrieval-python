# Contributing Guidelines

Contributions to `dataretrieval` are welcome and greatly appreciated, but
please read this document for information on *how* to contribute.

`dataretrieval` broadly follows a ["forking" workflow](https://docs.github.com/en/get-started/quickstart/contributing-to-projects),
however writing code is not the only way to contribute.

---

## Table of Contents

- [Contributing Guidelines](#contributing-guidelines)
  - [Table of Contents](#table-of-contents)
  - [Bugs](#bugs)
    - [Reporting Bugs](#reporting-bugs)
    - [Fixing Bugs](#fixing-bugs)
  - [Code Contributions](#code-contributions)
    - [Implementing Features](#implementing-features)
    - [Pull Request Guidelines](#pull-request-guidelines)
    - [Coding Standards and Style](#coding-standards-and-style)
      - [Style](#style)
      - [Docstrings](#docstrings)
      - [Quotes](#quotes)
  - [Documentation](#documentation)
    - [Contributing to the Documentation](#contributing-to-the-documentation)
    - [Adding Examples to the Documentation](#adding-examples-to-the-documentation)
  - [Feedback and Feature Requests](#feedback-and-feature-requests)
    - [Submitting Feedback](#submitting-feedback)
    - [Feature Requests](#feature-requests)
  - [Acknowledgements](#acknowledgements)

---

## Bugs

### Reporting Bugs

Report bugs at https://github.com/USGS-python/dataretrieval/issues.

When reporting a bug, please include:

* Detailed steps to reproduce the bug
* Your operating system name and version
* The Python version, as well as information about your local Python
  environment, such as the versions of installed packages
* Any additional details about your local setup that might be helpful in
  troubleshooting

### Fixing Bugs

Look through the GitHub [issues](https://github.com/USGS-python/dataretrieval/issues)
for known and unresolved bugs. Any issues labeled "bug" that are unassigned,
are open for resolution. You are welcome to comment in the relevant issue to
state your intention to resolve the bug, which will help ensure there is no
duplication of the same work by multiple contributors.

---

## Code Contributions

Code contributions should be made following a ["forking" workflow](https://docs.github.com/en/get-started/quickstart/contributing-to-projects).
This means that first, one should *fork* the repository, essentially creating a
personal mirror of the project. Next, you will want to create a *feature branch*
in your fork, which you can push code changes to. Once you have completed your
modifications and additions, open a pull request from the *feature branch* in
your fork, to the original upstream repository.

### Implementing Features

Look through the GitHub [issues](https://github.com/USGS-python/dataretrieval/issues)
for outstanding feature requests. Anything tagged with "enhancement"
and "please-help" is open to whomever wants to implement it.

Please do not combine multiple feature enhancements into a single pull request.

### Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. If the pull request adds or modifies package functionality, unit tests
   should be written to test the new functionality
2. If the pull request adds or modifies functionality, the documentation should
   be updated. To do so, either add or modify a functions docstring which will
   automatically become part of the API documentation
3. The pull request should work for Python 3.7, 3.8, 3.9, 3.10 - refer to the
   [python-package.yml file](https://github.com/USGS-python/dataretrieval/blob/master/.github/workflows/python-package.yml)
   for the latest versions of Python being tested by the continuous integration
   pipelines. This will be checked automatically by the CI pipelines once the
   pull request is opened.

### Coding Standards and Style

Note that coding standards and style as described below are strong suggestions,
the `dataretrieval` project does not strictly lint or enforce style guidelines
via any automated processes or pipelines.

#### Style

* Attempt to write code following the [PEP8 style guidelines](https://peps.python.org/pep-0008/) as much as possible
* The public interace should emphasize functions over classes; however, classes
  can and should be used internally and in tests
* Functions for downloading data from a specific web portal must be grouped
  within their own submodule
  * For example, all NWIS functions are located at `dataretrieval.nwis`

#### Docstrings
* Docstrings should follow the [numpy standard](https://numpydoc.readthedocs.io/en/v1.5.0/format.html):
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

#### Quotes

* Quote conventions follow http://stackoverflow.com/a/56190/5549:

  * Use double quotes around strings that are used for interpolation or that
    are natural language messages
  * Use single quotes for small symbol-like strings (but break the rules if
    the strings contain quotes)
  * Use triple double quotes for docstrings and raw string literals for
    regular expressions even if they aren't needed
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

---

## Documentation

### Contributing to the Documentation

Documentation is built using [sphinx](https://www.sphinx-doc.org/en/master/),
and is located within the `docs/source/` subdirectory in the repository.
Documentation is written using [reStructuredText](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html).

Contributions to the documentation should be made in a similar fashion to code
contributions - by following a forking workflow. When opening a pull request
please be sure to have tested your documentation modifications locally, and
clearly describe what it is your proposed changes add or fix.

### Adding Examples to the Documentation

A number of examples are provided in the documentation in the form of Jupyter
notebooks. These example notebooks are all contained within the `demos/`
subdirectory of the repository. If you have an example use of the package you
would like to add to the documentation as a run and rendered notebook, you
will need to do the following (in a separate branch of the repository):

1. Add your notebook to the `demos/` subdirectory after clearing all outputs
2. Add a corresponding `.nblink` file to `docs/source/examples/` subdirectory,
   see existing examples for reference, or refer to the [nbsphinx-link](https://nbsphinx-link.readthedocs.io/en/latest/) documentation.
3. Add the example and some text describing it to one of the `.rst` files in
   the examples subdirectory.
4. Run the documentation locally to ensure it renders as you expect, and then
   open a pull request wherein you describe the proposed addition.

---

## Feedback and Feature Requests

### Submitting Feedback

The best way to send feedback is to open an issue at
https://github.com/USGS-python/dataretrieval/issues.

Please be as clear as possible in your feedback, if you are reporting a bug
refer to [Reporting Bugs](#reporting-bugs).


### Feature Requests

To request or propose a new feature, open an issue at
https://github.com/USGS-python/dataretrieval/issues.

Please be sure to:
* Explain in detail how it would work, possibly with pseudo-code or an example
  use-case
* Keep the scope of the proposed feature as narrow as possible
* Make clear whether you would like to implement this feature, you need help
  devising the implementation, or you are unable to implement the feature
  yourself but would like it as a user

---

## Acknowledgements
This document was adapted from the cookiecutter project's [CONTRIBUTING file](https://github.com/audreyr/cookiecutter/blob/master/CONTRIBUTING.rst).
