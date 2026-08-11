.. _config:

dataretrieval.configuration
---------------------------

Layered configuration: a ``dataretrieval.configure(...)`` block, then
the ``API_USGS_*`` environment variables, then
``~/.dataretrieval/config.toml``, then built-in defaults. See the
:doc:`configuration guide </userguide/configuration>` for the settings and
worked examples.

.. automodule:: dataretrieval.configuration
    :members: configure, Configuration, BaseConfiguration, show_configuration,
              config_path, settings_for, ConfigurationError
    :show-inheritance:
