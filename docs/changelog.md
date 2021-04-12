# Changelog

### 2.6.0 - Better control of `Session`

 - A single `Session` object is now managed by the `AzmlClient` instance. This object is created by default, and possibly configured with the information from the `ClientConfig`. It is automatically closed when the client instance is garbaged out. A custom `Session` can be passed instead of the automatically-created one. In that case it is not automatically configured from the `ClientConfig`, but it is easy to do so using `<config>.configure_session(session)`. Fixes [#15](https://github.com/smarie/python-azureml-client/issues/15) and [#16](https://github.com/smarie/python-azureml-client/issues/16).

 - `create_session_for_proxy_from_strings` is deleted and `create_session_for_proxy` is deprecated. Users should now use `set_http_proxy(session, ...)` as the preferred way to configure a new `requests.Session()` object.

 - Fixed a bug (`Series` has no `items()` attribute) on some versions of pandas.

 - Migration to Github Actions.

### 2.5.0 - New `replace_NaN_with` and `replace_NaT_with` options

 - New `replace_NaN_with` and `replace_NaT_with` options to control how `NaN` and `NaT` get converted. Fixes [#11](https://github.com/smarie/python-azureml-client/issues/11)
 - packaging improvements: set the "universal wheel" flag to 1, and cleaned up the `setup.py`. In particular removed dependency to `six` for setup and added `py.typed` file. Added pyproject.toml too. Fixes [#13](https://github.com/smarie/python-azureml-client/issues/13)

### 2.4.0 - new helper method + doc

New method `create_response_body` in `RequestResponseClient`. Updated doc to show how these goodies can be used.

### 2.3.2 - added `__version__` attribute

Added `__version__` at package level.

### 2.3.1 - Config files can now be templates

`ClientConfig.load_config()` now accepts keyword arguments that will be used as replacements for variables in the config files. Fixes [#10](https://github.com/smarie/python-azureml-client/issues/10).

New dependency: `jinja2`

### 2.2.0 - Request-Response: new default behaviour concerning outputs, and new parameter.

Low-level API:

 - `RequestResponseClient.read_response_json_body` now does not filter outputs anymore even if a non-None `output_names` is provided.
 
 - New parameter to `execute_rr`: `only_keep_selected_output_names`. If this is `True` the legacy behaviour of filtering outputs by names is preserved. Otherwise all outputs are preserved. Fixes [#9](https://github.com/smarie/python-azureml-client/issues/9)

High-level API:

 - `AzureMLClient.call_azureml`: `ws_output_names` now appears as optional in the type hints. Improved docstring comments slightly.

### 2.1.0 - High-level clients: swagger, remote-only services + Bugfix 

New:

 * Swagger format is now supported in both high-level (call modes) and low-level (RR and Batch clients) API. Fixes [#5](https://github.com/smarie/python-azureml-client/issues/5).

 * New argument `remote_only` to disable local usage of a service. Fixes [#8](https://github.com/smarie/python-azureml-client/issues/8).

Misc:

 * Fixed bug with decoding AzureML errors. Fixes [#7](https://github.com/smarie/python-azureml-client/issues/7).

 * `call_local_service`: renamed argument `service_name` to `function_name` to distinguish better between the service (azureml) and the function (local implementation's method).


### 2.0.0 - New tools for high level api creation

New features:

 * `AzureMLClient` helper class to create pythonic high-level APIs, supporting both local and remote call modes, and configurable from yaml and ini/cfg files.

Refactoring: 

 * `azure-storage` dependency is now optional (only for batch mode)

 * improved documentation and type hints, and changed some method names and contents to be more pythonic. This was indeed a quite old project :).
 
 * removed `use_new_ws` from all methods - this concept is not meaningful anymore for AzureML usage.
 
 * renamed `RR_Client` into `RequestResponseClient` and `Batch_Client` into `BatchClient`. Old names will stay around for a version or two for compatibility reasons. 

 * got rid of the useless `Converters` container classes for data binding.

### 1.2.0 - Support for "swagger" mode 

 * Added `swagger=True` mode support for azureML calls

### 1.1.0 - Python 2 support, and a few bugfixes 

 * Updated this old project (my first public python package :) ) to more recent continuous integration and tests standards.
 * Support for python 2
 * A few bugfixes
 * Added all correct dependencies in `setup.py`
 
### 1.0.1 - First version

Request-response and Batch modes support, python 3 only.
