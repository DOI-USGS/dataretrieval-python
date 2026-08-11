.. _config:

dataretrieval.configuration
---------------------------

Layered configuration: a ``dataretrieval.configure(...)`` block holding one
configuration per adapter, then the ``API_USGS_*`` environment variables, then
the adapter's table and the package-wide keys in
``~/.dataretrieval/config.toml``, then built-in defaults. A
``[<adapter>.<name>]`` table is a named profile, selected in code with
``<Adapter>Configuration.load("<name>")``. See the
:doc:`configuration guide </userguide/configuration>` for the settings and
worked examples.

.. automodule:: dataretrieval.configuration
    :members: configure, Configuration, BaseConfiguration, show_configuration,
              config_path, settings_for, ConfigurationError
    :show-inheritance:
