# azmlclient

*An ***unofficial*** generic client stack for AzureML web services, working with both python 2 and 3.*

[![Python versions](https://img.shields.io/pypi/pyversions/azmlclient.svg)](https://pypi.python.org/pypi/azmlclient/) [![Build Status](https://travis-ci.org/smarie/python-azureml-client.svg?branch=master)](https://travis-ci.org/smarie/python-azureml-client) [![Tests Status](https://smarie.github.io/python-azureml-client/junit/junit-badge.svg?dummy=8484744)](https://smarie.github.io/python-azureml-client/junit/report.html) [![codecov](https://codecov.io/gh/smarie/python-azureml-client/branch/master/graph/badge.svg)](https://codecov.io/gh/smarie/python-azureml-client)

[![Documentation](https://img.shields.io/badge/doc-latest-blue.svg)](https://smarie.github.io/python-azureml-client/) [![PyPI](https://img.shields.io/pypi/v/azmlclient.svg)](https://pypi.python.org/pypi/azmlclient/) [![Downloads](https://pepy.tech/badge/azmlclient)](https://pepy.tech/project/azmlclient) [![Downloads per week](https://pepy.tech/badge/azmlclient/week)](https://pepy.tech/project/azmlclient) [![GitHub stars](https://img.shields.io/github/stars/smarie/python-azureml-client.svg)](https://github.com/smarie/python-azureml-client/stargazers)

!!! success "New `AzureMLClient` base class to create high-level clients is here, [check it out](#2-providing-high-level-apis)"

`azmlclient` helps you consume web services deployed on the AzureML platform easily. It provides you with a [low-level API](#1-low-level-api) to call web services in request-response or batch mode. It also offers optional tools if you wish to provide [high-level applicative APIs](#2-providing-high-level-apis) on top of these web services. 


As opposed to [AzureML client library](https://github.com/Azure/Azure-MachineLearning-ClientLibrary-Python#services-usage), 

 * this library is *much simpler* and is only focused on *consuming* web services. 
 * It is compliant with all services deployed from AzureML experiments (using the AzureML studio UI), and should also work with python and R "dataframe" web services (not checked though). 
 * It does not require your AzureML workspace id and API key, only the deployed services' URL and API key.

You may use it for example 

 * to show to your customers how to consume your AzureML cloud services.
 * to make simple 'edge' devices consume your AzureML cloud services (if they support python :) ).

## Installing

```bash
> pip install azmlclient
```

## 1. Low level API

This API is the python equivalent of the "execute" generic AzureML operation. It supports both request-response and batch mode, as well as swagger and non-swagger format.

### First examples

First create variables holding the endpoint information provided by AzureML

```python
base_url = 'https://<geo>.services.azureml.net/workspaces/<wId>/services/<sId>'
api_key = '<apiKey>'
```

Then create 

 * the inputs - a dictionary containing all you inputs as `pandas.DataFrame` objects
 * the parameters - a dictionary
 * and optionally define a list of expected output names
        
```python
inputs = {"trainDataset": training_df, "input2": input2_df}
params = {"param1": "val1", "param2": "val2"}
output_names = ["my_out1","my_out2"]
```

Finally call in Request-Response mode:

```python
from azmlclient import execute_rr
outputs = execute_rr(api_key, base_url, 
                     inputs=inputs, params=params, output_names=output_names)
```

Or in Batch mode. In this case you also need to configure the Blob storage to be used:

```python
from azmlclient import execute_bes

# Define the blob storage to use for storing inputs and outputs
blob_account = '<account_id>'       # 'myblobs'
blob_apikey = '<api_key>'           # 'mi3Qxcd5rwuM9r5k7h2ipXNww2T0Bw=='
blob_container = '<container>'      # 'rootcontainer'
blob_path_prefix = '<path_prefix>'  # 'folder/path'

# Perform the call (polling is done by default every 5s until job end)
outputs = execute_bes(api_key, base_url,
                      blob_account, blob_apikey, blob_container, 
                      blob_path_prefix=blob_path_prefix,
                      inputs=inputs, params=params, output_names=output_names)
```

### Debug and proxies

Users may wish to create a requests session object using the helper method provided, in order to override environment variable settings for HTTP requests. For example to use [`Fiddler`](https://www.telerik.com/fiddler) as a proxy to debug the web service calls: 

```python
from azmlclient import create_session_for_proxy
session = create_session_for_proxy(http_proxyhost='localhost', 
                                   http_proxyport=8888, 
                                   use_http_for_https_proxy=True,
                                   ssl_verify=False)
```

Then you may use that object in the `requests_session` parameter of the methods: 

```python
outputsRR = execute_rr(..., requests_session=session)
outputsB = execute_bes(..., requests_session=session)
```

Note that the session object will be passed to the underlying azure blob storage client to ensure consistency.

### Advanced usage

Advanced users may with to create `BatchClient` or `RequestResponseClient` classes to better control what's happening.

```python
from azmlclient import RequestResponseClient

# 0- Create the client
rr_client = RequestResponseClient(requests_session=requests_session)

# 1- Create the query body
request_body = rr_client.create_request_body(inputs, params)

# 2- Execute the query and receive the response body
response_body = rr_client.execute_rr(base_url, api_key, request_body)
# 3- parse the response body into a dictionary of dataframes
result_dfs = rr_client.read_response_json_body(response_body, output_names)
```

## 2. Providing high-level APIs

Even though the above API is enough to consume your AzureML web services, it is still very low-level: 

 * the services are not mapped to python methods with friendly names
 * their inputs, outputs and parameters have to be created by hand from python structures
 * changing the call mode between request-response and batch requires you to change your code
 * there is no easy way to switch between remote and local call, for example for hybrid implementations (computationally intensive operations in the cloud, computationally cheap operations executed locally)

For all these reasons, `azmlclient` offers tools to help you provide higher-level APIs. 

### Creating the main client class

Let's imagine that we have **two AzureML services** deployed: one for *adding dataframe columns* and another for *subtracting them*. We wish to provide our users with a more pythonic way to call them than the [low-level api](#1-low-level-api) that we saw previously.

A nice way to do this is to create a **"client class"**, that will hide away the AzureML specific syntax. We will name our class `MathsProvider`, it will offer one pythonic method mapped on each AzureML service: `add_columns(a_name, b_name, df)` and `subtract_columns(a_name, b_name, df)` respectively.
 
It is extremely easy to create such a class, by inheriting from `AzureMLClient`. This helper base class provide a bunch of mechanisms to automate both configuration and support for alternate call modes (local, request-response, batch) as we'll see below.

For each service that we want to offer, we create a method. That method should

 * be decorated with `@azureml_service`,
 * transform the received arguments (python objects) into azureml inputs and parameters dictionaries, in the same format that presented previously in the [low-level api](#1-low-level-api),
 * use the `self.call_azureml(...)` helper function to perform the AzureML call. Note that this helper function handles the call mode (request response or batch) for you as we'll see below.
 * unpack the various results and create the appropriate outputs (python objects) from them.

For example:

```python
from azmlclient import AzureMLClient, azureml_service

class MathsProvider(AzureMLClient):
    """
    A client for the `add_columns` and `subtract_columns` AzureML web services
    """
    @azureml_service
    def add_columns(self, a_name, b_name, df):
        """
        Offers a pythonic API around the `add_columns` azureML service

        :param a_name: name of the first column to add (a string)
        :param b_name: name of the second column to add (a string)
        :param df: the input dataframe, that should at least contain the 2 columns selected
        :return:
        """
        # (1) create the web service inputs and parameters from provided data.
        ws_inputs = {'input': df}
        ws_params = {'a_name': a_name, 'b_name': b_name}

        # (2) call the azureml web service
        result_dfs = self.call_azureml(self.add_columns,
                                       ws_inputs=ws_inputs, 
                                       ws_params=ws_params,
                                       ws_output_names=['output']  # optional
                                       )

        # (3) unpack the results
        return result_dfs['output']

    @azureml_service
    def subtract_columns(self, a_name, b_name, df):
        # (similar contents than `add_columns` here) 
        pass
```

### Using it

Using your new client is extremely easy: simply instantiate it with a `ClientConfig` configuration object describing the AzureML services endpoints and you're set:

```python
from azmlclient import ClientConfig, GlobalConfig, ServiceConfig
import pandas as pd

# create a configuration indicating the endpoints for each service id
cfg = ClientConfig(add_columns=ServiceConfig(base_url="https://.....", 
                                             api_key="...."),
                   subtract_columns=ServiceConfig(base_url="https://.....", 
                                                  api_key="...."))

# instantiate the client
client = MathsProvider(cfg)

# use it
df = pd.DataFrame({'x': [1, 2, 3], 'y': [0, 5, 10]})
result_df = client.add_columns('x', 'y', df)
```

The configuration object can alternately be loaded from a `.yaml` file such as [this one](https://github.com/smarie/python-azureml-client/blob/master/azmlclient/tests/clients/dummy/ws_client/dummy_client_conf.yaml):
 
```python
cfg = ClientConfig.load_yaml(yaml_file_path)
```

or from a `configparser`-compliant `.ini`/`.cfg` file such as [this one](https://github.com/smarie/python-azureml-client/blob/master/azmlclient/tests/clients/dummy/ws_client/dummy_client_conf.cfg):

```python
cfg = ClientConfig.load_config(cfg_file_path)
```

Note that the service names in the configuration are by default the method names in your client class. If you wish to use different names, simply provide the service name to the `@azureml_service` decorator, for example:

```python
    @azureml_service('subtract_columns')
    def minus_columns(self, a_name, b_name, df):
        ...
```

### Debugging

If you wish to debug the calls made by your client, there are two things that you can do:

 * (recommended) use a tool to capture network traffic such as Fiddler or Wireshark. Some tools such as Fiddler require you to change to http(s) proxy. This can be done in the `ClientConfig` object, by passing a `GlobalConfig`. See [this example](https://github.com/smarie/python-azureml-client/blob/master/azmlclient/tests/clients/dummy/ws_client/dummy_client_conf.cfg) for the properties to be set.
 
 * alternatively you can use the `with client.debug_requests()` context manager on your client. This will print the http requests contents on stdout:
 
```python
with client.debug_requests():
    result_df = client.add_columns('x', 'y', df)
```

### Alternate call modes: local, batch..

In the example above, `client.add_columns` calls the web service in request-response mode. This call mode can be changed temporarily thanks to the context managers provided:

```python
# change to BATCH mode
with client.batch_calls(polling_period_seconds=20):
    result_df = client.add_columns('x', 'y', df)

# change to RR mode (useless since that's already the default)
with client.rr_calls():
    result_df = client.add_columns('x', 'y', df)

# change to LOCAL mode
with client.local_calls():
    result_df = client.add_columns('x', 'y', df)
```

For the local calls by default it does not work and yields:

```python
NotImplementedError: Local execution is not available for this client. 
Please override `__init_local_impl__` or set a non-none `self._local_impl` 
if you wish local calls to be made available
```

But if you override the `__init_local_impl__` method and return an object on which the methods are available, it works:

```python
class MathsProviderLocal(object):
    """
    A local implementation of the same services
    """
    def add_columns(self, a_name, b_name, df):
        return pd.DataFrame({'sum': df[a_name] + df[b_name]})

    def subtract_columns(self, a_name, b_name, df):
        return pd.DataFrame({'diff': df[a_name] - df[b_name]})

class MathsProvider(AzureMLClient):
    def __init_local_impl__(self):
        """ Use our local implementation in 'local' call mode"""
        return MathsProviderLocal()

    @azureml_service
    def add_columns(self, a_name, b_name, df):
        ...

    @azureml_service
    def subtract_columns(self, a_name, b_name, df):
        ...
```

we can test it :

```python
>>> with client.local_calls():
>>>    result_df = client.add_columns('x', 'y', df)
>>> print(result_df)

   sum
0    1
1    7
2   13
```

Note that the default call mode can also be changed permanentlyby specifying another mode in the `AzureMLClient` constructor arguments, or by changing the `client._current_call_mode` attribute.


## Main features

 * Creates the Web Services requests from dataframe inputs and dataframe/dictionary parameters, and maps the responses to dataframes too
 * Maps the errors to more friendly python exceptions
 * Supports both Request/Response and Batch mode
 * In Batch mode, performs all the Blob storage and retrieval for you.
 * Properly handles file encoding in both modes (`utf-8` is used by default as the pivot encoding)
 * Supports global `requests.Session` configuration to configure the HTTP clients behaviour (including the underlying blob storage client).
 * Provides tools to create higher-level clients supporting both remote and local call modes. 

## See Also

 - The official [AzureML client library](https://github.com/Azure/Azure-MachineLearning-ClientLibrary-Python#services-usage)

### Others

*Do you like this library ? You might also like [my other python libraries](https://github.com/smarie/OVERVIEW#python)* 

## Want to contribute ?

Details on the github page: [https://github.com/smarie/python-azureml-client](https://github.com/smarie/python-azureml-client)
