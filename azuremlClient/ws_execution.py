import ssl
import urllib
import json

import dateutil
import pandas
import typing
from datetime import datetime
from azure.storage.blob import BlobService
import numpy as np
import datetime
import tempfile
import os
import csv
import io

from matplotlib.pyplot import cla

class IllegalJobStateException(Exception):
    """ This is raised whenever a job has illegal state"""

class JobExecutionException(Exception):
    """ This is raised whenever a job ended in failed mode"""


def executeRequestResponse(apiKey, baseUrl, wsInputs_DfDict, parameters, outputNames, useFiddlerProxy,
                           useNewWebServices):
    """
    Utility method to execute an AzureMl web service in request response mode

    :param apiKey:
    :param baseUrl:
    :param wsInputs_DfDict:
    :param parameters:
    :param outputNames:
    :param useFiddlerProxy:
    :param useNewWebServices:
    :return:
    """

    # 1- Create the query body
    requestBody_JsonDict = RequestResponseExecution.createRequestJsonBody(wsInputs_DfDict, parameters)

    # 2- Execute the query and receive the response body
    responseBody = RequestResponseExecution.execute_requestresponse(baseUrl, apiKey, requestBody_JsonDict,
                                                                                 useNewWebServices=useNewWebServices,
                                                                                 useFiddler=useFiddlerProxy)
    # 3- parse the response body into a dictionary of dataframes
    resultDataframes = RequestResponseExecution.readResponseJsonBody(responseBody, outputNames)
    return resultDataframes


def executeBatch(apiKey, baseUrl, blob_storage_account, blob_storage_apikey, blob_container_for_ios, wsInputs_DfDict,
                 parameters, outputNames, useFiddlerProxy, useNewWebServices):
    """
    Utility method to execute an azureML web service in batch mode

    :param apiKey:
    :param baseUrl:
    :param blob_storage_account:
    :param blob_storage_apikey:
    :param blob_container_for_ios:
    :param wsInputs_DfDict:
    :param parameters:
    :param outputNames:
    :param useFiddlerProxy:
    :param useNewWebServices:
    :return:
    """

    # 1- Transform the inputs into appropriate format.
    # NOTE : in batch the format is CSV not JSON !
    # wsInputs_JsonDict = BatchExecution.inputsDfDict_to_JsonDict(wsInputs_DfDict)
    wsInputs_CsvDict = BatchExecution.inputsDfDict_to_CsvDict(wsInputs_DfDict)

    # 2a- Push inputs to blob storage
    wsInputs_ReferenceDict, uniqueBlobNamePrefix = BatchExecution.pushAllInputsToBlobStorage(wsInputs_CsvDict,
                                                                                    account_name=blob_storage_account,
                                                                                    account_key=blob_storage_apikey,
                                                                                    container_name=blob_container_for_ios,
                                                                                    blobPathPrefix='virtualSensorQueries/',
                                                                                    charset='utf-8'
                                                                                    )

    # 2b- Create outputs reference on blob storage
    wsOutputs_ReferenceDict = BatchExecution.createOutputReferences(outputNames, account_name=blob_storage_account,
                                                                                    account_key=blob_storage_apikey,
                                                                                    container_name=blob_container_for_ios,
                                                                                    uniqueBlobNamePrefix=uniqueBlobNamePrefix)

    # 3- Create the query body
    requestBody_JsonDict = BatchExecution.createRequestJsonBody(wsInputs_ReferenceDict, parameters, wsOutputs_ReferenceDict)

    # 4- Perform the call
    jsonJobId = None
    try:
        # -- a) create the job
        jsonJobId = BatchExecution.execute_batch_createJob(baseUrl, apiKey,
                                                                        requestBody_JsonDict,
                                                                        useNewWebServices=useNewWebServices,
                                                                        useFiddler=useFiddlerProxy)

        # -- b) start the job
        BatchExecution.execute_batch_startJob(baseUrl, apiKey, jsonJobId,
                                                           useNewWebServices=useNewWebServices,
                                                           useFiddler=useFiddlerProxy)

        # -- polling loop
        outputsDict = None
        while outputsDict is None:
            # -- c) poll job status
            statusOrResult = BatchExecution.execute_batch_getJobStatusOrResult(baseUrl, apiKey,
                                                                                            jsonJobId,
                                                                                            useNewWebServices=useNewWebServices,
                                                                                            useFiddler=useFiddlerProxy)

            # -- e) check the job status and read response into a dictionary
            outputsDict = BatchExecution.readStatusOrResultJson(statusOrResult)

    finally:
        # -- e) delete the job
        if not (jsonJobId is None):
            BatchExecution.execute_batch_deleteJob(baseUrl, apiKey,
                                                                jsonJobId,
                                                                useNewWebServices=useNewWebServices,
                                                                useFiddler=useFiddlerProxy)

    # 5- Retrieve the outputs
    resultDataframes = BatchExecution.readResponseJsonFilesByReference(outputsDict, outputNames)

    return resultDataframes


class BaseExecution(object):


    @staticmethod
    def inputsDfDict_to_CsvDict(inputDataframes: typing.Dict[str, pandas.core.frame.DataFrame]) -> typing.Dict[
        str, str]:
        """
        Helper method to create Csv AzureML web service inputs, from input dataframes

        :param inputDataframes: a dictionary containing input names and input content (each input content is a dataframe)
        :return: a dictionary containing the string representations of the Csv inputs to store on the blob storage
        """

        # serialize each input to CSV separately
        inputCsvDict = {}
        for inputName, inputDataframe  in inputDataframes.items():
            inputCsvDict[inputName] = inputDataframe.to_csv(path_or_buf=None, sep=',', decimal='.', na_rep='NA', index=False, date_format='%Y-%m-%dT%H:%M:%S.000%z')

        return inputCsvDict

    @staticmethod
    def inputsDfDict_to_JsonDict(inputDataframes: typing.Dict[str, pandas.core.frame.DataFrame]) -> typing.Dict[str, str]:
        """
        Helper method to create JSON AzureML web service inputs, from input dataframes

        :param inputDataframes: a dictionary containing input names and input content (each input content is a dataframe)
        :return: a dictionary containing the string representations of the JSON inputs to store on the blob storage
        """

        # fill the input dictionary
        inputs = BaseExecution.dataFramesDict_to_azureMlTablesDict(inputDataframes)

        # serialize each input to JSON separately
        inputJsonDict = {}
        for inputName, inputDict in inputs.items():
            inputJsonDict[inputName] = BaseExecution._serializeDictAsJson(inputDict)

        return inputJsonDict


    @staticmethod
    def dataFramesDict_to_azureMlTablesDict(dataframesDict: typing.Dict[str, pandas.core.frame.DataFrame]) -> typing.Dict[str, typing.Dict[str, typing.List]]:
        """
        Converts a dictionary of dataframes into a dictionary of dictionaries following the structure
        required for AzureML JSON conversion

        :param dataframesDict: a dictionary containing input names and input content (each input content is a dataframe)
        :return: a dictionary of tables represented as dictionaries
        """

        # check input
        if not isinstance(dataframesDict, dict) or dataframesDict is None:
            raise TypeError('dataframesDict should be a non-None dictionnary of dataframes, found: ' + type(dataframesDict))

        # init the dictionary
        resultsDict = {}

        # loop all provided resultsDict and add them as dictionaries with "ColumnNames" and "Values"
        for dfName, df in dataframesDict.items():
            if isinstance(df, pandas.core.frame.DataFrame):
                # create one dictionary entry for this input
                resultsDict[dfName] = {'ColumnNames': df.columns.values.tolist(), "Values": df.values.tolist()}
            else:
                raise TypeError('object should be a dataframe, found: ' + type(df) + ' for table object: ' + dfName)

        return resultsDict

    @staticmethod
    def azureMlTablesDict_to_dataFramesDict(dictDict: typing.Dict[str, typing.Dict[str, typing.List]]) -> typing.Dict[str, pandas.core.frame.DataFrame]:

        # check input
        if not isinstance(dictDict, dict) or dictDict is None:
            raise TypeError(
                'dictDict should be a non-None dictionnary of dictionaries, found: ' + type(dictDict))

        # init the dictionary
        resultsDict = {}

        # loop all provided resultsDict and add them as dictionaries with "ColumnNames" and "Values"
        for dfName, dictio in dictDict.items():
            if isinstance(dictio, dict):
                if 'ColumnNames' in dictio.keys() and 'Values' in dictio.keys():
                    values = dictio['Values']
                    if len(values) > 0:
                        # # create dataframe
                        # c = pandas.DataFrame(np.array(values), columns=dictio['ColumnNames'])
                        #
                        # # auto-parse dates and floats
                        # for column in dictio['ColumnNames']:
                        #     # try to parse as datetime
                        #     try:
                        #         c[column] = c[column].apply(dateutil.parser.parse)
                        #     except ValueError:
                        #         pass
                        #
                        #     #try to parse as float
                        #     # ...

                        # use pandas parser to infer most of the types
                        #for that we dump in a buffer in a CSV format
                        buffer = io.StringIO(initial_value='', newline='\n')
                        writer = csv.writer(buffer, dialect='unix')
                        writer.writerows([dictio['ColumnNames']])
                        writer.writerows(values)
                        resultsDict[dfName] = pandas.read_csv(io.StringIO(buffer.getvalue()), sep=',', decimal='.', infer_datetime_format=True, parse_dates=[0])
                        buffer.close()
                    else:
                        #empty dataframe
                        resultsDict[dfName] = pandas.DataFrame(columns=dictio['ColumnNames'])
                else:
                    raise ValueError(
                        'object should be a dictionary with two fields ColumnNames and Values, found: ' + str(
                            dictio.keys()) + ' for table object: ' + dfName)
            else:
                raise TypeError('object should be a dictionary with two fields ColumnNames and values, found: ' + type(dictio) + ' for table object: ' + dfName)

        return resultsDict

    @staticmethod
    def paramDf_to_Dict(paramsDataframe: pandas.core.frame.DataFrame) -> typing.Dict[str, str]:
        """
        Converts a parameter dataframe into a dictionary following the structure required for JSON conversion

        :param paramsDataframe: a dictionary of parameter names and values
        :return: a dictionary of parameter names and values
        """

        # check params
        if not isinstance(paramsDataframe, pandas.core.frame.DataFrame):
            raise TypeError('paramsDataframe should be a dataframe or None, found: ' + type(paramsDataframe))

        # convert into dictionary
        params = {}
        for paramName in paramsDataframe.columns.values:
            params[paramName] = paramsDataframe.at[0, paramName]
        return params


    @staticmethod
    def _serializeDictAsJson(bodyDict):
        jsonBodyStr = json.dumps(bodyDict, default=BaseExecution.__json_serial)
        return jsonBodyStr


    @staticmethod
    def __json_serial(obj):
        """
        JSON custom serializer for objects not serializable by default json code

        :param obj:
        :return:
        """

        if isinstance(obj, bool) or np.issubdtype(type(obj), bool):
            # Booleans are converted to string representation
            return bool(obj) #str(obj)
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, datetime) or np.issubdtype(type(obj), datetime):
            # Datetime are written as ISO format string
            return obj.isoformat()
        else:
            raise TypeError("Type not serializable : " + str(obj))


    @staticmethod
    def _azureMl_httpCall(api_key, requestJsonBodyStr, url, method, useFiddler, useNewWebServices):

        # TODO
        if useNewWebServices:
            raise Exception('The AzureML *new* web services are not supported')

        # then fill the information about the query to perform
        headers = {'Authorization': ('Bearer ' + api_key)}

        if not (requestJsonBodyStr is None):
            # first encode the string as bytes using the charset
            charset = 'utf-8'
            jsonBodyEncodedWithCharset = str.encode(requestJsonBodyStr, encoding=charset)
            headers['Content-Type'] = 'application/json; charset=' + charset
        else:
            jsonBodyEncodedWithCharset = None


        # finally execute
        jsonResult = BaseExecution._httpCall(jsonBodyEncodedWithCharset, headers, method, url, useFiddler)

        return jsonResult


    @staticmethod
    def _httpCall(body, headers, method: str, url, useFiddler):
        """
        Sub-routine for HTTP web service call. If Body is None, a GET is performed

        :param body:
        :param headers:
        :param method
        :param url:
        :param useFiddler:
        :return:
        """

        try:
            if useFiddler:
                # for debug only : use fiddler proxy
                proxy = urllib.request.ProxyHandler({'http': '127.0.0.1:8888', 'https': '127.0.0.1:8888'})

                # accept fiddler's SSL certificate
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                https = urllib.request.HTTPSHandler(context=ctx)

                # chain the two options and install them
                opener = urllib.request.build_opener(proxy, https)
                urllib.request.install_opener(opener)

            # urlG = 'http://www.google.com/'
            # req = urllib.request.Request(urlG)
            # response = urllib.request.urlopen(req)

            # normal mode
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            response = urllib.request.urlopen(req)

            # read the response
            respbody = response.read()
            respcharset = response.headers.get_content_charset()
            if respcharset is None:
                # this is typically a 'no content' body but just to be sure read it with utf-8
                jsonResult = str(object=respbody, encoding='utf-8')
            else:
                jsonResult = str(object=respbody, encoding=response.headers.get_content_charset())

            return jsonResult

        except urllib.error.HTTPError as error:
            print("The request failed with status code: " + str(error.code))

            # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
            print(error.info())

            # Try to decode the error body and print it
            jsonError = str(object=error.read(), encoding=error.headers.get_content_charset())
            errorAsDict = json.loads(jsonError)
            print(json.dumps(errorAsDict, indent=4))

            if 'error' in errorAsDict:
                # this is an azureML standard error
                if errorAsDict['error']['code'] == 'LibraryExecutionError':
                    if errorAsDict['error']['details'][0]['code'] == 'TableSchemaColumnCountMismatch':
                        raise Exception(
                            'Dynamic schema validation is not supported in Request-Response mode, you should maybe use the BATCH response mode by setting useBatchMode to true in python')

            raise error

class RequestResponseExecution(BaseExecution):
    """
    A class providing static methods to perform Request-response calls to AzureML web services
    """

    @staticmethod
    def createRequestJsonBody(inputDataframes: typing.Dict[str, pandas.core.frame.DataFrame], paramsDataframe: pandas.core.frame.DataFrame) -> str:
        """
        Helper method to create a JSON AzureML web service input from inputs and parameters dataframes

        :param inputDataframes: a dictionary containing input names and input content (each input content is a dataframe)
        :param paramsDataframe: a dictionary of parameter names and values
        :return: a string representation of the request JSON body (not yet encoded in bytes)
        """

        # fill the inputs and params dictionaries
        # TODO maybe this method should be extracted so that users may check that the conversion to dict was ok. (and for symmetry with the Batch mode)
        inputs = BaseExecution.dataFramesDict_to_azureMlTablesDict(inputDataframes)
        params = BaseExecution.paramDf_to_Dict(paramsDataframe)

        # final body : combine them into a single dictionary ...
        bodyDict = {'Inputs': inputs, 'GlobalParameters': params}

        # ... and serialize as Json
        jsonBodyStr = BaseExecution._serializeDictAsJson(bodyDict)
        return jsonBodyStr


    @staticmethod
    def execute_requestresponse(baseUrl: str, api_key: str, requestJsonBodyStr: str, useNewWebServices: bool, useFiddler: bool) -> str:
        """
        Performs a web service call to AzureML using Request-response mode (synchronous, by value).
        Supports Fiddler capture for debug.

        :param baseUrl:
        :param api_key:
        :param requestJsonBodyStr: the json body of the web service request, as a string.
        :param useNewWebServices: a boolean to indicate to use the new AzureML web services
        :param useFiddler: a boolean to indicate to use Fiddler as the HTTP(s) proxy for the call
        :return: the json body of the response, as a string
        """

        requestResponseUrl = baseUrl + '/execute?api-version=2.0&details=true'

        jsonResult = BaseExecution._azureMl_httpCall(api_key, requestJsonBodyStr, requestResponseUrl, 'POST',
                                                               useFiddler, useNewWebServices)
        return jsonResult


    @staticmethod
    def readResponseJsonBody(jsonBodyStr: str, outputNames: typing.List[str]) -> typing.Dict[str, pandas.DataFrame]:
        """
        Reads a response body from a request-response web service call, into a dictionary of pandas dataframe

        :param jsonBodyStr: the response body, already decoded as a string
        :param outputNames: the names of the outputs to find. If empty, all outputs will be provided
        :return: the dictionary of corresponding dataframes mapped to the output names
        """

        # first read the json as a dictionary
        resultAsJsonDict = json.loads(jsonBodyStr)
        print(json.dumps(resultAsJsonDict, indent=4)) # TODO remove this print

        # then transform it into a dataframe
        resultAsDfDict = BaseExecution.azureMlTablesDict_to_dataFramesDict(resultAsJsonDict['Outputs'])
        if outputNames is None:
            return resultAsDfDict
        else:
            return resultAsDfDict[outputNames]

    @staticmethod
    def decodeRequestJsonBody(jsonBodyStr: str, ) -> typing.Tuple[pandas.DataFrame, typing.Dict]:
        """
        Reads a request body from a request-response web service call, into a dictionary of pandas dataframe + a dictionary of parameters

        :param jsonBodyStr:
        :return:
        """

        # first read the json as a dictionary
        resultAsJsonDict = json.loads(jsonBodyStr)

        return BaseExecution.azureMlTablesDict_to_dataFramesDict(resultAsJsonDict['Inputs']), resultAsJsonDict['GlobalParameters']

class BatchExecution(BaseExecution):
    """ This class provides static methods to call AzureML services in batch mode"""

    @staticmethod
    def pushAllInputsToBlobStorage(csvInputs: typing.Dict[str, str], account_name: str, account_key: str,
                                   container_name: str, blobPathPrefix: str, charset: str) -> typing.Tuple[typing.Dict[
        str, typing.Dict[str, str]], str]:
        """
        Utility method to push all inputs described in the provided dictionary into the selected blob storage on the cloud.
        Each input is an entry of the dictionary and containg the description of the input reference as dictionary.
        The string will be written to the blob using the provided charset.
        Note: files created on the blob storage will have names generated from the current time and the input name, and will be stored in

        :param csvInputs:
        :param account_name:
        :param account_key:
        :param container_name: the blob container name
        :param blobPathPrefix: the prefix
        :param charset:
        :return: a tuple containing (1) a dictionary of "by reference" input descriptions as dictionaries
                and (2) the unique naming prefix used to store the inputs
        """

        # setup the charset used for file encoding
        if charset is None:
            charset = 'utf-8'

        # check the blob path prefix
        if blobPathPrefix is None:
            blobPathPrefix = ""
        elif isinstance(blobPathPrefix, str):
            if not blobPathPrefix.endswith('/'):
                # append a trailing slash
                blobPathPrefix = blobPathPrefix + '/'
        else:
            raise TypeError('Blob path prefix should be a valid string or not be provided (default is empty string)')

        # create the Blob storage client for this account
        blob_service = BlobService(account_name=account_name, account_key=account_key)

        # unique naming prefix
        now = datetime.datetime.now()
        dtime = now.strftime("%Y-%m-%d_%H%M%S_%f")
        uniqueBlobNamePrefix = blobPathPrefix + dtime

        # A/ send all inputs to the blob storage
        inputBlobsNames = {}  # a variable to remember the blob names
        for inputName, inputJsonStr in csvInputs.items():

            # 0- open a temporary file on this computer to write the input
            (fileDescriptor, filePath) = tempfile.mkstemp()
            try:
                # 1- write the input to this file
                file = os.fdopen(fileDescriptor, mode='w', encoding=charset)
                file.write(inputJsonStr)

                # 2- push the file into an uniquely named blob on the cloud
                # -- generate unique blob name : use the date at the microsecond level

                blob_name = uniqueBlobNamePrefix + "-" + inputName + ".csv"
                # -- push the file to the blob storage
                blob_service.put_block_blob_from_path(container_name, blob_name, filePath)

                # 3- remember it
                inputBlobsNames[inputName] = container_name + "/" + blob_name

            except Exception as error:
                print('Error while writing input ' + inputName + ' to blob storage')
                raise error

            finally:
                # Whatever the situation, close the input file and delete it
                try:
                    os.close(fileDescriptor)
                finally:
                    os.remove(filePath)

        # B/ Finally create the description of these inputs "by reference"
        # note: this is separate from the above loop just in case we want to split it in a separate function later on
        connectionString = "DefaultEndpointsProtocol=https;AccountName=" + account_name + ";AccountKey=" + account_key
        inputByReference = {}
        for inputName, inputBlobName in inputBlobsNames.items():
            inputByReference[inputName] = {"ConnectionString": connectionString, "RelativeLocation": inputBlobName}

        return inputByReference, uniqueBlobNamePrefix


    @staticmethod
    def createOutputReferences(outputNames: typing.List[str], account_name: str, account_key: str, container_name: str, uniqueBlobNamePrefix:str) -> typing.Dict[str, typing.Dict[str,str]]:
        """
        Utility method to create output references

        :param account_name:
        :param account_key:
        :param container_name:
        :param outputNames:
        :return:
        """

        connectionString = "DefaultEndpointsProtocol=https;AccountName=" + account_name + ";AccountKey=" + account_key

        outputsByReference = {}
        for outputName in outputNames:
            outputsByReference[outputName] = {"ConnectionString": connectionString, "RelativeLocation": container_name + "/" + uniqueBlobNamePrefix + "-" + outputName + ".csv"}

        return outputsByReference


    @staticmethod
    def createRequestJsonBody(inputReferences: typing.Dict[str, typing.Dict[str, str]],
                              paramsDataframe: pandas.core.frame.DataFrame,
                              outputReferences: typing.Dict[str, typing.Dict[str, str]]) -> str:
        """
        Helper method to create a JSON AzureML web service input in Batch mode, from 'by reference' inputs, and parameters as dataframe

        :param inputReferences: a dictionary containing input names and input references (each input reference is a dictionary)
        :param paramsDataframe: a dictionary of parameter names and values
        :param outputReferences: a dictionary containing output names and output references (each output reference is a dictionary)
        :return: a string representation of the request JSON body (not yet encoded in bytes)
        """

        # fill the inputs and params dictionaries
        params = BaseExecution.paramDf_to_Dict(paramsDataframe)

        # final body : combine them into a single dictionary ...
        bodyDict = {'Inputs': inputReferences, 'GlobalParameters': params, 'Outputs': outputReferences}

        # ... and serialize as Json
        jsonBodyStr = BaseExecution._serializeDictAsJson(bodyDict)
        return jsonBodyStr



    @staticmethod
    def execute_batch_createJob(baseUrl: str, api_key: str, requestJsonBodyStr: str, useNewWebServices: bool, useFiddler: bool) -> str:
        """
        Performs a web service call to AzureML using Batch mode (asynchronous, by reference).
        Supports Fiddler capture for debug.

        :param baseUrl:
        :param api_key:
        :param requestJsonBodyStr:
        :param useNewWebServices:
        :param useFiddler:
        :return:
        """

        batchUrl = baseUrl + '/jobs?api-version=2.0'
        jsonJobId = BatchExecution._azureMl_httpCall(api_key, requestJsonBodyStr, batchUrl, method='POST',
                                                               useFiddler=useFiddler, useNewWebServices=useNewWebServices)

        # unquote the json Job Id
        if jsonJobId.startswith('"') and jsonJobId.endswith('"'):
            return jsonJobId[1:-1]
        else:
            return jsonJobId



    @staticmethod
    def execute_batch_startJob(baseUrl: str, api_key: str, jobIdStr: str, useNewWebServices: bool,
                               useFiddler: bool):
        """
        Starts an AzureML Batch job (asynchronous, by reference).
        Supports Fiddler capture for debug.

        :param baseUrl:
        :param api_key:
        :param jobIdStr:
        :param useNewWebServices:
        :param useFiddler:
        :return:
        """

        batchUrl = baseUrl + '/jobs/' + jobIdStr + '/start?api-version=2.0'

        BatchExecution._azureMl_httpCall(api_key, None, batchUrl, method='POST',
                                                      useFiddler=useFiddler, useNewWebServices=useNewWebServices)
        return


    @staticmethod
    def execute_batch_getJobStatusOrResult(baseUrl: str, api_key: str, jobIdStr: str, useNewWebServices: bool,
                                           useFiddler: bool) -> str:
        """
        Gets the status or the result of an AzureML Batch job (asynchronous, by reference).
        Supports Fiddler capture for debug.

        :param baseUrl:
        :param api_key:
        :param jobIdStr:
        :param useNewWebServices:
        :param useFiddler:
        :return:
        """

        batchUrl = baseUrl + '/jobs/' + jobIdStr + '?api-version=2.0'
        jsonJobStatusOrResult = BatchExecution._azureMl_httpCall(api_key, None, batchUrl, method='GET',
                                                               useFiddler=useFiddler, useNewWebServices=useNewWebServices)
        return jsonJobStatusOrResult


    # {"StatusCode":"Running","Results":null,"Details":null,"CreatedAt":"2016-10-14T14:52:33.979Z","StartTime":"2016-10-14T14:52:52.168Z","EndTime":"0001-01-01T00:00:00+00:00"}'
    @staticmethod
    def readStatusOrResultJson(jsonJobStatusOrResult: str) -> str:
        """
        Reads the status or the result of an AzureML Batch job (asynchronous, by reference).
        Throws an error if the status is an error, or an empty result if the status is a

        :param jsonJobStatusOrResult:
        :return: the status as a dictionary, and throws an error if the job had an error
        """

        # first read the json as a dictionary
        resultAsJsonDict = json.loads(jsonJobStatusOrResult)
        print(json.dumps(resultAsJsonDict, indent=4))  # TODO remove this print

        if resultAsJsonDict['StatusCode'] in ['3','Cancelled']:
            raise IllegalJobStateException('The job state is ' + resultAsJsonDict['StatusCode'] + ' : cannot read the outcome')

        elif resultAsJsonDict['StatusCode'] in ['2','Failed']:
            raise JobExecutionException('The job ended with an error : ' + resultAsJsonDict['Details'])

        elif resultAsJsonDict['StatusCode'] in ['0','NotStarted','1','Running','4','Finished']:
            jsonJobStatusOrResult = resultAsJsonDict['Results']

        else:
            raise IllegalJobStateException(
                'The job state is ' + resultAsJsonDict['StatusCode'] + ' : unknown state')

        return jsonJobStatusOrResult

    @staticmethod
    def execute_batch_deleteJob(baseUrl: str, api_key: str, jobIdStr: str, useNewWebServices: bool,
                               useFiddler: bool):
        """
        Deletes an AzureML Batch job (asynchronous, by reference).
        Supports Fiddler capture for debug.

        :param baseUrl:
        :param api_key:
        :param jobIdStr:
        :param useNewWebServices:
        :param useFiddler:
        :return:
        """

        batchUrl = baseUrl + '/jobs/' + jobIdStr + '?api-version=2.0'

        BatchExecution._azureMl_httpCall(api_key, None, batchUrl, method='DELETE',
                                                   useFiddler=useFiddler, useNewWebServices=useNewWebServices)
        return

    @staticmethod
    def readResponseJsonFilesByReference(jsonOutputs: typing.Dict[str, typing.Dict[str, str]], outputNames: typing.List[str]) -> typing.Dict[str, pandas.DataFrame]:
        """
        Reads responses from an AzureMl Batch web service call, into a dictionary of pandas dataframe

        :param jsonOutputs: the json output description by reference for each output
        :param outputNames: the names of the outputs to retrieve and read, or None for all
        :return: the dictionary of corresponding dataframes mapped to the output names
        """


        # then transform it into a dataframe
        resultAsDfDict = dict()
        if outputNames is None:
            for outputName, outputRefJson in jsonOutputs.items():
                # todo complete
                print(outputRefJson)
                resultAsDfDict[outputName] = pandas.DataFrame(None, columns=None)
        else:
            for outputName in outputNames:
                # todo complete
                print(jsonOutputs[outputName])
                resultAsDfDict[outputName] = pandas.DataFrame(None, columns=None)

        return resultAsDfDict