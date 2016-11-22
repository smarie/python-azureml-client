# python-azureml-client
a generic client stack for azureML web services, working with python 3 (https://github.com/Azure/Azure-MachineLearning-ClientLibrary-Python#services-usage)


## 1. Overview

## What's new


## 2. Getting started

### *A - Installing the package*

#### Recommended : create a clean virtual environment

Note: we strongly recommend that you create a new conda environment (or a pip virtualenv) before installing in order to avoid packages conflicts. Once you are in a virtual environment, open a terminal and check that the python is really the one from your virtual environment:

    (Windows)>  where python
    (Linux)$    which python

The first python executable that should show up should be the one from the environment you think you're in.

#### Simple install - 'let me just use it'

If you don't plan to improve and contribute to this package, the easiest way to go is the following commands, executed from a terminal (in your conda or virtualenv environment). You will be able to debug in the sources if required, but you won't be able to edit the code and contribute to the project.

* using **pip** to install from a release (wheel): **TODO**

* using **pip** to install from sources ([ref](https://packaging.python.org/installing/#installing-from-vcs)):

        pip install git+https://github.com/smarie/python-azureml-client.git#egg=azmlclient-1.0.0.dev1


#### Installation for developers - 'let me use it and I might help to improve it'

If you want to be able to edit and commit improvements on github, then you'll have to perform the two steps separately. First let's clone the project : in PyCharm

    *VCS > Git > Clone* *https://github.com/smarie/python-azureml-client.git* (accept 'open the project in current window' with 'add to current project' option)

Alternatively you can do it using the following commandline but you'll have to import the project later in PyCharm:

    cd <your_workspace_parent_folder>
    git clone https://github.com/smarie/python-azureml-client.git
      
Then you may install the package from the local folder in editable mode (meaning that your modifications in the code will be taken into account - you may need to restart the python terminal though) ([ref](https://packaging.python.org/installing/#installing-from-a-local-src-tree)):    
            
    pip install -e <path_to_python-azureml-client_folder>


#### Note for conda users

The only drawback of the methods above using pip, is that during install all dependencies (numpy, scikit-learn, etc.) are installed using *pip* too, and therefore are not downloaded from validated *conda* repositories. If you prefer to install them from *conda*, the workaround is to run the following command **before** to execute the above installation:

    conda install numpy, pandas, azure-storage==0.33.0
    

#### Uninstalling

As usual : 

    pip uninstall azmlclient
    

### C - Examples

First create variables holding the access information provided by AzureML

    baseUrl = 'https://europewest.services.azureml.net/workspaces/<workspaceId>/services/<serviceId>'
    apiKey = '<apiKey>'
    useNewWebServices = False

Then create a dictionary containing all you inputs, as dataframe objects

    wsInputs_DfDict = {"trainDataset": trainingData, "input2": input2, }
    outputNames = ["model","trainingSet","diagInfo","modelPerformance","trainingSetStatistics", "driversUsed"]
