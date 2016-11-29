# python-azureml-client
A generic client stack for azureML web services, working with python 3.
 
Contrary to the 'complete' AzureML client library (https://github.com/Azure/Azure-MachineLearning-ClientLibrary-Python#services-usage), this simple library is only focused on calling the deployed web services using python 3. It does not require your AzureML workspace id and API key, only the deployed services' URL and API key.

You may use it for example 
* to show to your customers how to consume your AzureML cloud services.
* to make simple 'edge' devices (with python support :) ) consume your AzureML cloud services.


## Main features

* Creates the Web Services requests from dataframe inputs and dataframe/dictionary parameters, and maps the responses to dataframes too
* Maps the errors to more friendly python exceptions
* Supports both Request/Response and Batch mode
* In Batch mode, performs all the Blob storage and retrieval for you.
* Properly handles file encoding in both modes


## Examples

First create variables holding the access information provided by AzureML

    ```python
    baseUrl = 'https://europewest.services.azureml.net/workspaces/<workspaceId>/services/<serviceId>'
    apiKey = '<apiKey>'
    useNewWebServices = <False/True>
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
    outputs = ac.executeRequestResponse(apiKey, baseUrl, inputs=inputs, params=params, outputNames=outputNames)
    ```

Or in Batch mode. In this case you also need to configure the Blob storage to be used:

    ```python
    # Define the blob storage to use for storing inputs and outputs
    blob_account = '<account_id>'
    blob_apikey = '<api_key>'
    blob_container = '<container>'
    blob_path_prefix = '<path_prefix>'
    
    # Perform the call (polling is done every 5s until job end)
    outputs = ac.executeBatch(apiKey, baseUrl,
                            blob_storage_account, blob_storage_apikey, blob_container_for_ios, blob_path_prefix=blob_path_prefix,
                            inputs=inputs, params=params, outputNames=outputNames)
    ```

## Advanced usage

Advanced users may directly use the static methods in *BatchExecution* and *RequestResponseExecution* classes to better control what's happening.

Also two optional parameters allow to work with a local Fiddler proxy (*useFiddler=True*) and with the 'new web services' mode (*useNewWebService=True* - still evolving on MS side, so will need to be updated).


## Installing the package

### Recommended : create a clean virtual environment

We strongly recommend that you create a new conda *environment* or pip *virtualenv*/*venv* before installing in order to avoid packages conflicts. Once you are in your virtual environment, open a terminal and check that the python interpreter is correct:

    ```bash
    (Windows)>  where python
    (Linux)  $  which python
    ```

The first executable that should show up should be the one from the virtual environment.


### Simple install

If you don't plan to improve and contribute to this package, the easiest way to go is the following commands. You will be able to debug in the sources if required, but you won't be able to edit the code and contribute to the project.

* using **pip** to install from a release (wheel): **TODO**

* using **pip** to install from git sources ([ref](https://packaging.python.org/installing/#installing-from-vcs)):

        ```bash
        pip install git+https://github.com/smarie/python-azureml-client.git#egg=azmlclient-1.0.0.dev1
        ```

### Note for conda users

The only drawback of the methods above using pip, is that during install all dependencies (numpy, pandas, azure-storage) are installed using *pip* too, and therefore are not downloaded from validated *conda* repositories. If you prefer to install them from *conda*, the workaround is to run the following command **before** to execute the above installation:

    ```bash
    conda install numpy, pandas, azure-storage==0.33.0
    ```

### Uninstalling

As usual : 

    ```bash
    pip uninstall azmlclient
    ```

