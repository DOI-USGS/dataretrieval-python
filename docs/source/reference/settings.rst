.. _config:

dataretrieval.settings
---------------------------

Layered configuration: a ``dataretrieval.configure(...)`` block holding one
configuration per adapter, then the ``API_USGS_*`` environment variables, then
the adapter's table and the package-wide keys in
``~/.dataretrieval/config.toml``, then built-in defaults. A
``[<adapter>.<name>]`` table is a named profile, selected in code with
``<Adapter>Settings.load("<name>")``. See the
:doc:`settings guide </userguide/settings>` for the settings and
worked examples.

.. automodule:: dataretrieval.settings
    :members: configure, Settings, AdapterSettings, show_settings,
              config_path, settings_for, ConfigurationError
    :show-inheritance:
