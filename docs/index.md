# azmlclient

*An ***unofficial*** generic client stack for azureML web services, working with both python 2 and 3.*

[![Python versions](https://img.shields.io/pypi/pyversions/azmlclient.svg)](https://pypi.python.org/pypi/azmlclient/) [![Build Status](https://travis-ci.org/smarie/python-azmlclient.svg?branch=master)](https://travis-ci.org/smarie/python-azmlclient) [![Tests Status](https://smarie.github.io/python-azmlclient/junit/junit-badge.svg?dummy=8484744)](https://smarie.github.io/python-azmlclient/junit/report.html) [![codecov](https://codecov.io/gh/smarie/python-azmlclient/branch/master/graph/badge.svg)](https://codecov.io/gh/smarie/python-azmlclient)

[![Documentation](https://img.shields.io/badge/doc-latest-blue.svg)](https://smarie.github.io/python-azmlclient/) [![PyPI](https://img.shields.io/pypi/v/azmlclient.svg)](https://pypi.python.org/pypi/azmlclient/) [![Downloads](https://pepy.tech/badge/azmlclient)](https://pepy.tech/project/azmlclient) [![Downloads per week](https://pepy.tech/badge/azmlclient/week)](https://pepy.tech/project/azmlclient) [![GitHub stars](https://img.shields.io/github/stars/smarie/python-azmlclient.svg)](https://github.com/smarie/python-azmlclient/stargazers)

As opposed to [AzureML client library](https://github.com/Azure/Azure-MachineLearning-ClientLibrary-Python#services-usage), 

 * this library is much simpler and is only focused on consuming web services. 
 * It is compliant with all services deployed from AzureML experiments (using the AzureML drag'n drop UI), and should also work with python and R "dataframe" web services (not checked though). 
 * It does not require your AzureML workspace id and API key, only the deployed services' URL and API key.

You may use it for example 

 * to show to your customers how to consume your AzureML cloud services.
 * to make simple 'edge' devices consume your AzureML cloud services (if they support python :) ).

## Installing

```bash
> pip install azmlclient
```

## Usage

### First examples

First create variables holding the endpoint information provided by AzureML

```python
base_url = 'https://europewest.services.azureml.net/workspaces/<workspaceId>/services/<serviceId>'
api_key = '<apiKey>'
use_new_ws = False
```

Then create 

 * the inputs - a dictionary containing all you inputs as dataframe objects
 * the parameters - a dictionary
 * and optionally provide a list of expected output names
        
```python
inputs = {"trainDataset": trainingDataDf, "input2": input2Df}
params = {"param1": "val1", "param2": "val2"}
output_names = ["my_out1","my_out2"]
```

Finally call in Request-Response mode:

```python
from azmlclient import execute_rr
outputs = execute_rr(api_key, base_url, inputs=inputs, params=params, output_names=output_names)
```

Or in Batch mode. In this case you also need to configure the Blob storage to be used:

```python
from azmlclient import execute_bes

# Define the blob storage to use for storing inputs and outputs
blob_account = '<account_id>'
blob_apikey = '<api_key>'
blob_container = '<container>'
blob_path_prefix = '<path_prefix>'

# Perform the call (polling is done every 5s until job end)
outputs = execute_bes(api_key, base_url,
                          blob_storage_account, blob_storage_apikey, blob_container_for_ios, 
						  blob_path_prefix=blob_path_prefix,
                          inputs=inputs, params=params, output_names=output_names)
```

### Debug and proxies

Users may wish to create a requests session object using the helper method provided, in order to override environment variable settings for HTTP requests. For example to use `Fiddler` as a proxy to debug the web service calls: 

```python
from azmlclient import create_session_for_proxy
session = create_session_for_proxy(http_proxyhost='localhost', http_proxyport=8888, 
									  use_http_for_https_proxy=True, ssl_verify=False)
```

Then you may use that object in the `requests_session` parameter of the methods: 

```python
outputsRR = execute_rr(..., requests_session=session)
outputsB = execute_bes(..., requests_session=session)
```

Note that the session object will be passed to the underlying azure blob storage client to ensure consistency.

### Advanced usage

Advanced users may directly create `Batch_Client` or `RR_Client` classes to better control what's happening.

An optional parameter allow to work with the 'new web services' mode (`use_new_ws = True` - still evolving on MS side, so will need to be updated).


## Main features

* Creates the Web Services requests from dataframe inputs and dataframe/dictionary parameters, and maps the responses to dataframes too
* Maps the errors to more friendly python exceptions
* Supports both Request/Response and Batch mode
* In Batch mode, performs all the Blob storage and retrieval for you.
* Properly handles file encoding in both modes (`utf-8` is used by default as the pivot encoding)
* Supports global `requests.Session` configuration to configure the HTTP clients behaviour (including the underlying blob storage client).

## See Also

 - The official [AzureML client library](https://github.com/Azure/Azure-MachineLearning-ClientLibrary-Python#services-usage)

### Others

*Do you like this library ? You might also like [my other python libraries](https://github.com/smarie/OVERVIEW#python)* 

## Want to contribute ?

Details on the github page: [https://github.com/smarie/python-azmlclient](https://github.com/smarie/python-azmlclient)
