# python-azureml-client

An ***unofficial*** generic client stack for azureML web services, working with python 3.
 
As opposed to the 'complete' [AzureML client library](https://github.com/Azure/Azure-MachineLearning-ClientLibrary-Python#services-usage), this library is much simpler and is only focused on calling the deployed web services using python 3. It does not require your AzureML workspace id and API key, only the deployed services' URL and API key.

You may use it for example 
* to show to your customers how to consume your AzureML cloud services.
* to make simple 'edge' devices consume your AzureML cloud services (if they support python :) ).


## Main features

* Creates the Web Services requests from dataframe inputs and dataframe/dictionary parameters, and maps the responses to dataframes too
* Maps the errors to more friendly python exceptions
* Supports both Request/Response and Batch mode
* In Batch mode, performs all the Blob storage and retrieval for you.
* Properly handles file encoding in both modes (`utf-8` is used by default as the pivot encoding)
* Supports global `requests.Session` configuration to configure the HTTP clients behaviour (including the underlying blob storage client).


## Examples

First import the package

```python
import azmlclient as ac  
```

Then create variables holding the access information provided by AzureML

```python
base_url = 'https://europewest.services.azureml.net/workspaces/<workspaceId>/services/<serviceId>'
api_key = '<apiKey>'
use_new_ws = False
```

Then create 
* the inputs - a dictionary containing all you inputs as dataframe objects
        
    ```python
    inputs = {"trainDataset": trainingDataDf, "input2": input2Df}
    ```
        
* the parameters - a dictionary
        
    ```python
    params = {"param1": "val1", "param2": "val2"}
    ```

* and optionally provide a list of expected output names
        
    ```python
    outputNames = ["my_out1","my_out2"]
    ```

Finally call in Request-Response mode:

```python
outputs = ac.execute_rr(api_key, base_url, inputs=inputs, params=params, output_names=output_names)
```

Or in Batch mode. In this case you also need to configure the Blob storage to be used:

```python
# Define the blob storage to use for storing inputs and outputs
blob_account = '<account_id>'
blob_apikey = '<api_key>'
blob_container = '<container>'
blob_path_prefix = '<path_prefix>'

# Perform the call (polling is done every 5s until job end)
outputs = ac.execute_bes(api_key, base_url,
                          blob_storage_account, blob_storage_apikey, blob_container_for_ios, blob_path_prefix=blob_path_prefix,
                          inputs=inputs, params=params, output_names=output_names)
```

## Debug and proxies

Users may wish to create a requests session object using the helper method provided, in order to override environment variable settings for HTTP requests. For example to use `Fiddler` as a proxy to debug the web service calls: 

```python
session = ac.create_session_for_proxy(http_proxyhost='localhost', http_proxyport=8888, use_http_for_https_proxy=True, ssl_verify=False)
```

Then you may use that object in the `requests_session` parameter of the methods: 

```python
outputsRR = ac.execute_rr(..., requests_session=session)
outputsB = ac.execute_bes(..., requests_session=session)
```

Note that the session object will be passed to the underlying azure blob storage client to ensure consistency.

## Advanced usage

Advanced users may directly create `Batch_Client` or `RR_Client` classes to better control what's happening.

An optional parameter allow to work with the 'new web services' mode (`use_new_ws = True` - still evolving on MS side, so will need to be updated).


## Installation

### Recommended : create a clean virtual environment

We strongly recommend that you use conda *environment* or pip *virtualenv*/*venv* in order to better manage packages. Once you are in your virtual environment, open a terminal and check that the python interpreter is correct:

```bash
(Windows)>  where python
(Linux)  >  which python
```

The first executable that should show up should be the one from the virtual environment.


### Simple install

* using **pip** to install from a release

    ```bash
    > pip install azmlclient
    ```
* using **pip** to install from git sources ([ref](https://packaging.python.org/installing/#installing-from-vcs)):

    ```bash
    > pip install git+https://github.com/smarie/python-azureml-client.git#egg=azmlclient-1.0.0.dev1
    ```

### Note for conda users

The only drawback of the methods above using pip, is that during install all dependencies (numpy, pandas, azure-storage) are installed using *pip* too, and therefore are not downloaded from validated *conda* repositories. If you prefer to install them from *conda*, the workaround is to run the following command **before** to execute the above installation:

```bash
> conda install numpy, pandas, azure-storage==0.33.0
```

### Uninstalling

As usual : 

```bash
> pip uninstall azmlclient
```

## Developers

### Packaging

This project uses `setuptools_scm` to synchronise the version number. Therefore the following command should be used for development snapshots as well as official releases: 

```bash
python setup.py egg_info bdist_wheel rotate -m.whl -k3
```

### Releasing memo

```bash
twine upload dist/* -r pypitest
twine upload dist/*
```