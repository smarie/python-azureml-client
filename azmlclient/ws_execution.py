import csv
import io
import json
import os
import ssl
import tempfile
import time
import typing
import urllib
from datetime import datetime
from io import StringIO

import numpy as np
import pandas
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings


class IllegalJobStateException(Exception):
    """ This is raised whenever a job has illegal state"""

class JobExecutionException(Exception):
    """ This is raised whenever a job ended in failed mode"""

class AzmlException(Exception):
    """
    Represents an AzureMl exception, built from an HTTP error body received from AzureML.
    """

    def __init__(self, httpError: urllib.error.HTTPError):

        # Try to decode the error body and print it
        jsonError = str(object=httpError.read(), encoding=httpError.headers.get_content_charset())
        errorAsDict = json.loads(jsonError)

        # store it for reference
        self.__errorAsDict = errorAsDict


    def __str__(self):

        # if 'error' in self.__errorAsDict:
        #     # this is an azureML standard error
        #     if self.__errorAsDict['error']['code'] == 'LibraryExecutionError':
        #         if self.__errorAsDict['error']['details'][0]['code'] == 'TableSchemaColumnCountMismatch':
        #             return 'Dynamic schema validation is not supported in Request-Response mode, you should maybe use the BATCH response mode by setting useBatchMode to true in python'

        return json.dumps(self.__errorAsDict, indent=4)


def executeRequestResponse(apiKey: str, baseUrl: str, inputs: typing.Dict[str, pandas.DataFrame]=None,
                           params= None, outputNames: typing.List[str]=None,
                           useFiddlerProxy: bool=False, useNewWebServices:bool=False) -> typing.Dict[str, pandas.DataFrame]:
    """
    Utility method to execute an AzureMl web service in request response mode

    :param apiKey: the api key for the service to call
    :param baseUrl: the URL of the service to call
    :param inputs: an optional dictionary containing the inputs, by name. Inputs should be dataframes.
    :param params: an optional dictionary containing the parameters by name, or a dataframe containing the parameters.
    :param outputNames: an optional list of expected output names
    :param useFiddlerProxy: if True, calls will use localhost:8888 as a proxy, with deactivated SSL certificate validation, so that you may debug your calls using Fiddler.
    :param useNewWebServices: if True, calls will use the AzureML 'new Web services' format
    :return: a dictionary of outputs, by name. Outputs are dataframes
    """

    # 1- Create the query body
    requestBody_JsonDict = RequestResponseExecution.createRequestJsonBody(inputs, params)

    # 2- Execute the query and receive the response body
    responseBody = RequestResponseExecution.execute_requestresponse(baseUrl, apiKey, requestBody_JsonDict,
                                                                                 useNewWebServices=useNewWebServices,
                                                                                 useFiddler=useFiddlerProxy)
    # 3- parse the response body into a dictionary of dataframes
    resultDataframes = RequestResponseExecution.readResponseJsonBody(responseBody, outputNames)
    return resultDataframes


def executeBatch(apiKey, baseUrl, blob_storage_account, blob_storage_apikey, blob_container_for_ios,
                            blob_path_prefix:str = None, blob_charset:str=None,
                            inputs: typing.Dict[str, pandas.DataFrame]=None,
                            params= None, outputNames: typing.List[str]=None,
                            nbSecondsBetweenJobStatusQueries:int=5,
                            useFiddlerProxy: bool=False, useNewWebServices:bool=False):
    """
    Utility method to execute an azureML web service in batch mode. Job status is queried every 5 seconds by default, you may wish to change that number.

    :param apiKey: the api key for the service to call
    :param baseUrl: the URL of the service to call
    :param blob_storage_account: the storage account to use to store the inputs and outputs
    :param blob_storage_apikey: the storage api key to use to store the inputs and outputs
    :param blob_container_for_ios: the container in the blob storage, that will be used to store the inputs and outputs
    :param blob_path_prefix: an optional prefix that will be used to store the blobs
    :param blob_charset: optional encoding of files used on the blob storage
    :param inputs: an optional dictionary containing the inputs, by name. Inputs should be dataframes.
    :param params: an optional dictionary containing the parameters by name, or a dataframe containing the parameters.
    :param outputNames: an optional list of expected output names
    :param useFiddlerProxy: if True, calls will use localhost:8888 as a proxy, with deactivated SSL certificate validation, so that you may debug your calls using Fiddler.
    :param useNewWebServices: if True, calls will use the AzureML 'new Web services' format
    :return: a dictionary of outputs, by name. Outputs are dataframes
    """


    blob_service = BlockBlobService(account_name=blob_storage_account, account_key=blob_storage_apikey)

    # 1- Push inputs to blob storage and create output references
    print('Pushing inputs to blob storage')
    inputRefs, outputRefs = BatchExecution.pushInputsToBlobStorage_and_CreateOutputsReferences(inputs,
                                                                                               outputNames=outputNames,
                                                                                               blob_service=blob_service,
                                                                                               blob_container=blob_container_for_ios,
                                                                                               blob_path_prefix=blob_path_prefix,
                                                                                               charset=blob_charset)

    # 2- Create the query body
    requestBody_JsonDict = BatchExecution.createRequestJsonBody(inputRefs, params, outputRefs)

    # 3- Perform the call
    jsonJobId = None
    try:
        # -- a) create the job
        print('Creating job')
        jsonJobId = BatchExecution.execute_batch_createJob(baseUrl, apiKey, requestBody_JsonDict,
                                                           useNewWebServices=useNewWebServices,
                                                           useFiddler=useFiddlerProxy)

        # -- b) start the job
        print('Starting job ' + str(jsonJobId))
        BatchExecution.execute_batch_startJob(baseUrl, apiKey, jsonJobId,
                                              useNewWebServices=useNewWebServices,
                                              useFiddler=useFiddlerProxy)
        print('Job ' + str(jsonJobId) + ' started')

        # -- polling loop
        outputsDict = None
        while outputsDict is None:
            # -- c) poll job status
            print('Polling job status for job ' + str(jsonJobId))
            statusOrResult = BatchExecution.execute_batch_getJobStatusOrResult(baseUrl, apiKey, jsonJobId,
                                                                               useNewWebServices=useNewWebServices,
                                                                               useFiddler=useFiddlerProxy)

            # -- e) check the job status and read response into a dictionary
            outputsDict = BatchExecution.readStatusOrResultJson(statusOrResult)

            # wait
            print('Waiting ' + str(nbSecondsBetweenJobStatusQueries) + 's until next call.')
            time.sleep(nbSecondsBetweenJobStatusQueries)

    finally:
        # -- e) delete the job
        if not (jsonJobId is None):
            print('Deleting job ' + str(jsonJobId))
            BatchExecution.execute_batch_deleteJob(baseUrl, apiKey, jsonJobId,
                                                   useNewWebServices=useNewWebServices,
                                                   useFiddler=useFiddlerProxy)

    # 4- Retrieve the outputs
    print('Job ' + str(jsonJobId) + ' completed, results: ')
    print(json.dumps(outputsDict, indent=4))

    print('Retrieving the outputs from the blob storage')
    # dont use the output of the status, it does not contain the connectionString
    # resultDataframes = ByReference_Converters.readResponseJsonFilesByReference(outputsDict)
    resultDataframes = Collection_Converters.BlobCsvRefDict_to_DfDict(outputRefs)

    return resultDataframes


def _check_not_none_and_typed(var, varType=None, varName=None):
    """
    Helper method to check that an object is not none and possibly f a certain type

    :param var: the object
    :param varType: the type
    :param varName: the name of the varioable to be used in error messages
    :return:
    """

    if (var is None):
        if varName is None:
            raise TypeError('Error, object should be non-None')
        else:
            raise TypeError('Error, object with name "'+varName+'" should be non-None')
    elif not (varType is None):
        if not isinstance(var, varType):
            if varName is None:
                raise TypeError('Error, object should be a ' + varType + ', found: ' + str(
                    type(var)))
            else:
                raise TypeError('Error, object with name "' + varName + '" should be a ' + varType + ', found: ' + str(
                    type(var)))
    return


class Converters(object):

    @staticmethod
    def Df_to_Csv(df: pandas.DataFrame, dfName:str = None, charset:str = None) -> str:
        """
        Converts the provided dataframe to a csv, to store it on blob storage for AzureML calls.
        WARNING: datetime columns are converted in ISO format but the milliseconds are ignored and set to zero.

        :param df:
        :param dfName:
        :return:
        """
        _check_not_none_and_typed(df, varType=pandas.DataFrame, varName=dfName)

        # TODO what about timezone detail if not present, will the %z be ok ?
        return df.to_csv(path_or_buf=None, sep=',', decimal='.', na_rep='', encoding=charset,
                         index=False, date_format='%Y-%m-%dT%H:%M:%S.000%z')

    @staticmethod
    def Csv_to_Df(csv_buffer_or_filepath: str, csvName: str = None) -> pandas.DataFrame:
        """
        Converts the provided csv compliant with AzureML Batch calls, to a dataframe

        :param csv_buffer_or_filepath:
        :param csvName:
        :return:
        """
        _check_not_none_and_typed(csv_buffer_or_filepath, varName=csvName)

        # TODO how to parse and set timezone correctly to utc without knowing which is the datetime column ?
        return pandas.read_csv(csv_buffer_or_filepath, sep=',', decimal='.', infer_datetime_format=True, parse_dates=True)


    @staticmethod
    def Df_to_AzmlTable(df: pandas.DataFrame, isAzureMlOutput: bool = False, dfName: str = None) -> typing.Dict[str, str]:
        """
        Converts the provided Dataframe to a dictionary in the same format than the JSON expected by AzureML in the
        Request-Response services. Note that contents are kept as is (values are not converted to string yet)

        :param df:
        :param dfName:
        :return:
        """
        _check_not_none_and_typed(df, varType=pandas.DataFrame, varName=dfName)

        if isAzureMlOutput:
            # use this method recursively, in 'not output' mode
            return {'type': 'table', 'value': Converters.Df_to_AzmlTable(df, dfName=dfName)}
        else:
            return {'ColumnNames': df.columns.values.tolist(), "Values": df.values.tolist()}


    @staticmethod
    def AzmlTable_to_Df(azmlTableDict: typing.Dict[str, str], isAzureMlOutput: bool = False, tableName: str = None) \
            -> pandas.DataFrame:
        """
        Converts an AzureML table (JSON-like dictionary) into a dataframe. Since two formats exist (one for inputs and
        one for outputs), there is a parameter you can use to specify which one to use.

        :param isAzureMlOutput:
        :param tableName:
        :return:
        """
        _check_not_none_and_typed(azmlTableDict, varType=dict, varName=tableName)

        if isAzureMlOutput:
            if 'type' in azmlTableDict.keys() and 'value' in azmlTableDict.keys():
                if azmlTableDict['type'] == 'table':
                    # use this method recursively, in 'not output' mode
                    return Converters.AzmlTable_to_Df(azmlTableDict['value'], tableName=tableName)
                else:
                    raise ValueError('This method is able to read table objects, found type=' + azmlTableDict['type'])
            else:
                raise ValueError(
                    'object should be a dictionary with two fields "type" and "value", found: ' + str(
                        azmlTableDict.keys()) + ' for table object: ' + tableName)
        else:
            if 'ColumnNames' in azmlTableDict.keys() and 'Values' in azmlTableDict.keys():
                values = azmlTableDict['Values']
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
                    # for that we dump in a buffer in a CSV format
                    buffer = io.StringIO(initial_value='', newline='\n')
                    writer = csv.writer(buffer, dialect='unix')
                    writer.writerows([azmlTableDict['ColumnNames']])
                    writer.writerows(values)
                    res = pandas.read_csv(io.StringIO(buffer.getvalue()), sep=',', decimal='.',
                                          infer_datetime_format=True,
                                          parse_dates=[0])
                    buffer.close()
                else:
                    # empty dataframe
                    res = pandas.DataFrame(columns=azmlTableDict['ColumnNames'])
            else:
                raise ValueError(
                    'object should be a dictionary with two fields ColumnNames and Values, found: ' + str(
                        azmlTableDict.keys()) + ' for table object: ' + tableName)
            return res


    @staticmethod
    def ParamDf_to_ParamDict(paramsDataframe: pandas.DataFrame) -> typing.Dict[str, str]:
        """
        Converts a parameter dataframe into a dictionary following the structure required for JSON conversion

        :param paramsDataframe: a dictionary of parameter names and values
        :return: a dictionary of parameter names and values
        """
        _check_not_none_and_typed(paramsDataframe, varType=pandas.DataFrame, varName='paramsDataframe')

        # params = {}
        # for paramName in paramsDataframe.columns.values:
        #     params[paramName] = paramsDataframe.at[0, paramName]
        # return params
        return { paramName: paramsDataframe.at[0, paramName] for paramName in paramsDataframe.columns.values }


    @staticmethod
    def ParamDict_to_ParamDf(paramsDict: typing.Dict[str, typing.Any]) -> pandas.DataFrame:
        """
        Converts a parameter dictionary into a parameter dataframe

        :param paramsDict:
        :return:
        """
        _check_not_none_and_typed(paramsDict, varType=dict, varName='paramsDict')

        return pandas.DataFrame(paramsDict, index=[0])


    @staticmethod
    def HttpError_to_AzmlError(httpError:urllib.error.HTTPError) -> AzmlException:
        return AzmlException(httpError)


# class BlobStorageContainer(object):
#
#     def __init__(self, blob_account_name: str, blob_account_key: str, blob_container_name: str):
#         self.account_name = blob_account_name
#         self.account_key = blob_account_key
#         self.container_name = blob_container_name
#
#     def getBlockBlobService(self):
#         lock = threading.RLock()
#         lock.acquire()
#         try:
#             if self.__blob_service is None:
#                 self.__blob_service = BlockBlobService(account_name=self.account_name, account_key=self.account_key)
#             return self.__blob_service
#         finally:
#             lock.release()


class ByReference_Converters(object):

    @staticmethod
    def _get_valid_blob_path_prefix(blob_path_prefix: str) -> str:
        """
        Utility method to get a valid blob path prefix from a provided one. A trailing slash is added if non-empty

        :param blob_path_prefix:
        :return:
        """
        if blob_path_prefix is None:
            blob_path_prefix = ''
        elif isinstance(blob_path_prefix, str):
            if len(blob_path_prefix) > 0 and not blob_path_prefix.endswith('/'):
                blob_path_prefix = blob_path_prefix + '/'
        else:
            raise TypeError('Blob path prefix should be a valid string or not be provided (default is empty string)')

        return blob_path_prefix

    @staticmethod
    def _get_valid_blob_name_prefix(blob_name_prefix: str) -> str:
        """
        Utility method to get a valid blob path prefix from a provided one. A trailing slash is added if non-empty

        :param blob_name_prefix:
        :return:
        """
        if blob_name_prefix is None:
            blob_name_prefix = ''
        elif isinstance(blob_name_prefix, str):
            if blob_name_prefix.__contains__('/') or blob_name_prefix.__contains__('\\'):
                raise ValueError('Blob name prefix should not contain / nor \\')
        else:
            raise TypeError('Blob path prefix should be a valid string or not be provided (default is empty string)')

        return blob_name_prefix

    @staticmethod
    def _getBlobServiceConnectionString(blob_service: BlockBlobService) -> str:
        """
        Utilty method to get the connection string for a blob storage service (currently the BlockBlobService does
        not provide any method to do that)

        :param blob_service:
        :return:
        """
        _check_not_none_and_typed(blob_service, BlockBlobService, 'blob_service')

        return 'DefaultEndpointsProtocol=https;AccountName=' + blob_service.account_name + ';AccountKey=' + blob_service.account_key


    @staticmethod
    def createBlobCsvRef(blob_service: BlockBlobService, blob_container: str,
                             blob_name: str, blob_path_prefix: str = None, blob_name_prefix: str = None) \
            -> typing.Dict[str, str]:
        """
        Utility method to create a reference to a blob, whether it exists or not

        :param blob_service:
        :param blob_container:
        :param blob_name:
        :param blob_path_prefix:
        :param blob_name_prefix:
        :return:
        """
        _check_not_none_and_typed(blob_container, str, 'blob_container')
        _check_not_none_and_typed(blob_name, str, 'blob_name')

        # fix the blob name
        if blob_name.lower().endswith('.csv'):
            blob_name = blob_name[:-4]

        # validate blob service and get conection string
        connectionString = ByReference_Converters._getBlobServiceConnectionString(blob_service)

        # check the blob path prefix, append a trailing slash if necessary
        blob_path_prefix = ByReference_Converters._get_valid_blob_path_prefix(blob_path_prefix)
        blob_name_prefix = ByReference_Converters._get_valid_blob_name_prefix(blob_name_prefix)

        # output reference
        return {'ConnectionString': connectionString,
                'RelativeLocation': blob_container + '/' + blob_path_prefix + blob_name_prefix + blob_name + '.csv'}


    @staticmethod
    def Csv_to_BlobCsvRef(csv_str: str, blob_service: BlockBlobService, blob_container: str,
                          blob_name: str, blob_path_prefix: str = None, blob_name_prefix: str = None,
                          charset: str = None) -> typing.Dict[str, str]:

        # setup the charset used for file encoding
        if charset is None:
            charset = 'utf-8'
        if charset != 'utf-8':
            print('Warning: blobs can be written in any charset but currently only utf-8 blobs may be read back into '
                  'dataframes. We recommend setting charset to None or utf-8 ')

        _check_not_none_and_typed(csv_str, str, 'csv_str')
        _check_not_none_and_typed(blob_name, str, 'blob_name')

        # 1- first create the reference in order to check everything is ok
        blob_reference = ByReference_Converters.createBlobCsvRef(blob_service=blob_service,
                                                                 blob_container=blob_container,
                                                                 blob_path_prefix=blob_path_prefix,
                                                                 blob_name_prefix=blob_name_prefix,
                                                                 blob_name=blob_name)

        # 2- open a temporary file on this computer to write the csv
        (fileDescriptor, filePath) = tempfile.mkstemp()
        try:
            # 3- write the input to this file
            file = os.fdopen(fileDescriptor, mode='w', encoding=charset)
            file.write(csv_str)
            file.flush()

            # 4- push the file into an uniquely named blob on the cloud
            # -- remove trailing '.csv': this is what is done in createBlobCsvRef, we have to redo it here
            if blob_name.lower().endswith('.csv'):
                blob_name = blob_name[:-4]
            blob_full_name = blob_path_prefix + blob_name_prefix + blob_name + '.csv'
            # -- push
            blob_service.create_blob_from_path(blob_container, blob_full_name, filePath,
                                               content_settings=ContentSettings(content_type='text.csv', content_encoding=charset))

            # 5- return reference
            return blob_reference

        except Exception as error:
            print('Error while writing input ' + blob_name + ' to blob storage')
            raise error

        finally:
            # Whatever the situation, close the input file and delete it
            try:
                os.close(fileDescriptor)
            finally:
                os.remove(filePath)


    @staticmethod
    def BlobCsvRef_to_Csv(blob_reference: dict, blob_name:str=None, encoding:str = None):
        """
        Reads a CSV referenced according to the format defined by AzureML, and transforms it into a Dataframe

        :param blob_reference:
        :param encoding:
        :return:
        """
        _check_not_none_and_typed(blob_reference, varType=dict, varName=blob_name)

        if not(encoding is None or encoding=='utf-8'):
            raise ValueError('Unsupported encoding to retrieve blobs : ' + encoding)

        if ('ConnectionString' in blob_reference.keys()) and ('RelativeLocation' in blob_reference.keys()):

            # create the Blob storage client for this account
            blob_service = BlockBlobService(connection_string=blob_reference['ConnectionString'])

            # find the container and blob path
            container, name = blob_reference['RelativeLocation'].split(sep='/', maxsplit=1)

            # retrieve it and convert
            # -- this works but is probably less optimized for big blobs that get chunked, than using streaming
            blob_string = blob_service.get_blob_to_text(blob_name=name, container_name=container)
            return blob_string.content

        else:
            raise ValueError('Blob reference is invalid: it should contain ConnectionString and RelativeLocation fields')

    @staticmethod
    def Df_to_BlobCsvRef(df: pandas.DataFrame, blob_service: BlockBlobService, blob_container: str,
                          blob_name: str, blob_path_prefix: str = None, blob_name_prefix: str = None,
                          charset: str = None) -> typing.Dict[str, str]:
        """
        right now in two steps : first create the csv, then upload it.

        :param df:
        :param blob_service:
        :param blob_container:
        :param blob_name:
        :param blob_path_prefix:
        :param blob_name_prefix:
        :param charset:
        :return:
        """

        # create the csv
        csv_str = Converters.Df_to_Csv(df, dfName=blob_name, charset=charset)

        # upload it
        return ByReference_Converters.Csv_to_BlobCsvRef(csv_str, blob_service=blob_service,
                                                             blob_container=blob_container,
                                                             blob_path_prefix=blob_path_prefix,
                                                             blob_name_prefix=blob_name_prefix,
                                                             blob_name=blob_name, charset=charset)

    @staticmethod
    def BlobCsvRef_to_Df(blob_reference: dict, blob_name: str = None, encoding: str = None):
        """
        Reads a CSV blob referenced according to the format defined by AzureML, and transforms it into a Dataframe

        :param blob_reference:
        :param encoding:
        :return:
        """

        # TODO copy the BlobCsvRef_to_Csv method here and handle the blob in streaming mode to be big blobs chunking-compliant.
        # However how to manage the buffer correctly, create the StringIO with correct encoding, and know the number of chunks
        # that should be read in pandas.read_csv ? A lot to dig here to get it right...
        #
        # from io import TextIOWrapper
        # contents = TextIOWrapper(buffer, encoding=charset, ...)
        # blob = blob_service.get_blob_to_stream(blob_name=name, container_name=container, encoding=charset, stream=contents)

        blob_content = ByReference_Converters.BlobCsvRef_to_Csv(blob_reference, blob_name=blob_name, encoding=encoding)

        if len(blob_content) > 0:
            return Converters.Csv_to_Df(StringIO(blob_content), blob_name)
        else:
            return pandas.DataFrame()

class Collection_Converters(object):

    @staticmethod
    def createBlobCsvRefDict(blob_service: BlockBlobService, blob_container: str,
                             blob_names: typing.List[str], blob_path_prefix: str = None,
                             blob_name_prefix:str = None) -> typing.Dict[str, typing.Dict[str, str]]:
        """
        Utility method to create one or several blob references on the same container on the same blob storage service.

        :param blob_service:
        :param blob_container:
        :param blob_names:
        :param blob_path_prefix: optional prefix to the blob names
        :param blob_name_prefix:
        :return:
        """
        _check_not_none_and_typed(blob_names, list, 'blob_names')

        # output dict of references
        return {blobName: ByReference_Converters.createBlobCsvRef(blob_service, blob_container, blobName,
                                                                  blob_path_prefix=blob_path_prefix,
                                                                  blob_name_prefix=blob_name_prefix)
                for blobName in blob_names}

    @staticmethod
    def DfDict_to_CsvDict(dataframesDict: typing.Dict[str, pandas.DataFrame], charset: str = None) -> typing.Dict[
        str, str]:
        """
        Helper method to create CSVs compliant with AzureML web service BATCH inputs, from a dictionary of input dataframes

        :param dataframesDict: a dictionary containing input names and input content (each input content is a dataframe)
        :return: a dictionary containing the string representations of the Csv inputs to store on the blob storage
        """
        _check_not_none_and_typed(dataframesDict, varType=dict, varName='dataframesDict')

        return {inputName: Converters.Df_to_Csv(inputDf, dfName=inputName, charset=charset) for inputName, inputDf in dataframesDict.items()}


    @staticmethod
    def CsvDict_to_DfDict(csvsDict: typing.Dict[str, str]) -> typing.Dict[str, pandas.DataFrame]:
        """
        Helper method to read CSVs compliant with AzureML web service BATCH inputs/outputs, into a dictionary of Dataframes

        :param csvsDict:
        :return:
        """
        _check_not_none_and_typed(csvsDict, varType=dict, varName='csvsDict')

        return {inputName: Converters.Csv_to_Df(inputCsv, csvName=inputName) for inputName, inputCsv in csvsDict.items()}


    @staticmethod
    def DfDict_to_AzmlTablesDict(dataframesDict: typing.Dict[str, pandas.DataFrame]) -> typing.Dict[
        str, typing.Dict[str, typing.List]]:
        """
        Converts a dictionary of dataframes into a dictionary of dictionaries following the structure
        required for AzureML JSON conversion

        :param dataframesDict: a dictionary containing input names and input content (each input content is a dataframe)
        :return: a dictionary of tables represented as dictionaries
        """
        _check_not_none_and_typed(dataframesDict, varType=dict, varName='dataframesDict')

        # resultsDict = {}
        # for dfName, df in dataframesDict.items():
        #     resultsDict[dfName] = Converters.Df_to_AzmlTable(df, dfName)
        # return resultsDict

        return { dfName: Converters.Df_to_AzmlTable(df, dfName=dfName) for dfName, df in dataframesDict.items() }


    @staticmethod
    def AzmlTablesDict_to_DfDict(azmlTablesDict: typing.Dict[str, typing.Dict[str, typing.List]],
                                 isAzureMlOutput: bool = False) -> typing.Dict[str, pandas.DataFrame]:

        _check_not_none_and_typed(azmlTablesDict, varType=dict, varName='azmlTablesDict')

        return { dfName: Converters.AzmlTable_to_Df(dictio, isAzureMlOutput=isAzureMlOutput, tableName=dfName)
                 for dfName, dictio in azmlTablesDict.items()}


    @staticmethod
    def BlobCsvRefDict_to_CsvDict(blobcsvReferences: typing.Dict[str, typing.Dict[str, str]], charset: str = None) \
            -> typing.Dict[str, str]:

        _check_not_none_and_typed(blobcsvReferences, dict, 'blobcsvReferences')

        return {blobName: ByReference_Converters.BlobCsvRef_to_Csv(csvBlobRef, encoding=charset, blob_name=blobName)
                for blobName, csvBlobRef in blobcsvReferences.items()}


    @staticmethod
    def CsvDict_to_BlobCsvRefDict(csvsDict: typing.Dict[str, str], blob_service: BlockBlobService,
                                  blob_container: str, blob_path_prefix: str = None,
                                  blob_name_prefix: str = None, charset: str = None) \
            -> typing.Dict[str, typing.Dict[str, str]]:
        """
        Utility method to push all inputs described in the provided dictionary into the selected blob storage on the cloud.
        Each input is an entry of the dictionary and containg the description of the input reference as dictionary.
        The string will be written to the blob using the provided charset.
        Note: files created on the blob storage will have names generated from the current time and the input name, and will be stored in

        :param csvsDict:
        :param blob_service:
        :param blob_container:
        :param blob_path_prefix: the optional prefix that will be prepended to all created blobs in the container
        :param blob_name_prefix: the optional prefix that will be prepended to all created blob names in the container
        :param charset: an optional charset to be used, by default utf-8 is used
        :return: a dictionary of "by reference" input descriptions as dictionaries
        """

        _check_not_none_and_typed(csvsDict, dict, 'csvsDict')

        return {blobName: ByReference_Converters.Csv_to_BlobCsvRef(csvStr, blob_service=blob_service,
                                                             blob_container=blob_container,
                                                             blob_path_prefix=blob_path_prefix,
                                                             blob_name_prefix=blob_name_prefix,
                                                             blob_name=blobName, charset=charset)
                for blobName, csvStr in csvsDict.items()}



    @staticmethod
    def BlobCsvRefDict_to_DfDict(blobReferences: typing.Dict[str, typing.Dict[str, str]], charset: str = None) \
            -> typing.Dict[str, pandas.DataFrame]:
        """
        Reads Blob references, for example responses from an AzureMl Batch web service call, into a dictionary of
        pandas dataframe

        :param blobReferences: the json output description by reference for each output
        :return: the dictionary of corresponding dataframes mapped to the output names
        """
        _check_not_none_and_typed(blobReferences, dict, 'blobReferences')

        return {blobName: ByReference_Converters.BlobCsvRef_to_Df(csvBlobRef, encoding=charset, blob_name=blobName)
                for blobName, csvBlobRef in blobReferences.items()}

    @staticmethod
    def DfDict_to_BlobCsvRefDict(dataframesDict: typing.Dict[str, pandas.DataFrame], blob_service: BlockBlobService,
                                 blob_container: str, blob_path_prefix: str = None, blob_name_prefix: str = None,
                                 charset: str = None) -> typing.Dict[str, typing.Dict[str, str]]:

        _check_not_none_and_typed(dataframesDict, dict, 'dataframesDict')

        return {blobName: ByReference_Converters.Df_to_BlobCsvRef(csvStr, blob_service=blob_service,
                                                                   blob_container=blob_container,
                                                                   blob_path_prefix=blob_path_prefix,
                                                                   blob_name_prefix=blob_name_prefix,
                                                                   blob_name=blobName, charset=charset)
                for blobName, csvStr in dataframesDict.items()}

class BaseExecution(object):

    @staticmethod
    def _serializeDictAsJson(bodyDict):
        """
        Transforms a dictionary to a JSON string. Datetimes are converted using ISO format.

        :param bodyDict:
        :return:
        """
        jsonBodyStr = json.dumps(bodyDict, default=BaseExecution.__json_serial)
        return jsonBodyStr


    @staticmethod
    def __json_serial(obj):
        """
        JSON custom serializer for objects not serializable by default json code

        :param obj:
        :return:
        """
        if isinstance(obj, np.integer):
            # since ints are bool, do ints first
            return int(obj)
        elif isinstance(obj, bool):
            return bool(obj)
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
        """
        Utility method to perform an HTTP request to AzureML service.

        :param api_key:
        :param requestJsonBodyStr:
        :param url:
        :param method:
        :param useFiddler:
        :param useNewWebServices:
        :return:
        """
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
    def _httpCall(body, headers, method: str, url, useFiddler:bool=False):
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

            raise Converters.HttpError_to_AzmlError(error)


class RequestResponseExecution(BaseExecution):
    """
    A class providing static methods to perform Request-response calls to AzureML web services
    """

    @staticmethod
    def createRequestJsonBody(inputDataframes: typing.Dict[str, pandas.DataFrame]=None,
                              paramsDfOrDict: pandas.DataFrame=None) -> str:
        """
        Helper method to create a JSON AzureML web service input from inputs and parameters dataframes

        :param inputDataframes: a dictionary containing input names and input content (each input content is a dataframe)
        :param paramsDfOrDict: a dictionary of parameter names and values
        :return: a string representation of the request JSON body (not yet encoded in bytes)
        """

        if inputDataframes is None:
            inputDataframes = {}
        if paramsDfOrDict is None:
            paramsDfOrDict = {}

        # inputs
        inputs = Collection_Converters.DfDict_to_AzmlTablesDict(inputDataframes)

        # params
        if isinstance(paramsDfOrDict, dict):
            params = paramsDfOrDict
        elif isinstance(paramsDfOrDict, pandas.DataFrame):
            params = Converters.ParamDf_to_ParamDict(paramsDfOrDict)
        else:
            raise TypeError('paramsDfOrDict should be a dataframe or a dictionary, or None, found: ' + type(paramsDfOrDict))

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
    def readResponseJsonBody(jsonBodyStr: str, outputNames: typing.List[str]=None) -> typing.Dict[str, pandas.DataFrame]:
        """
        Reads a response body from a request-response web service call, into a dictionary of pandas dataframe

        :param jsonBodyStr: the response body, already decoded as a string
        :param outputNames: the names of the outputs to find. If empty, all outputs will be provided
        :return: the dictionary of corresponding dataframes mapped to the output names
        """

        # first read the json as a dictionary
        resultAsJsonDict = json.loads(jsonBodyStr)

        # then transform it into a dataframe
        resultAsDfDict = Collection_Converters.AzmlTablesDict_to_DfDict(resultAsJsonDict['Results'], isAzureMlOutput=True)

        # return the expected outputs
        if outputNames is None:
            return resultAsDfDict
        else:
            if len(set(outputNames) - set(resultAsDfDict.keys())) > 0:
                missings = list(set(outputNames) - set(resultAsDfDict.keys()))
                raise Exception(
                    'Error : the following outputs are missing in the results : ' + str(missings))
            else:
                slicedDictionary = {k: v for k, v in resultAsDfDict.items() if k in outputNames}
            return slicedDictionary


    @staticmethod
    def decodeRequestJsonBody(jsonBodyStr: str, ) -> typing.Tuple[typing.Dict[str, pandas.DataFrame], typing.Dict]:
        """
        Reads a request body from a request-response web service call, into a dictionary of pandas dataframe + a dictionary of parameters

        :param jsonBodyStr:
        :return:
        """

        # first read the json as a dictionary
        resultAsJsonDict = json.loads(jsonBodyStr)

        return Converters.AzmlTablesDict_to_DfDict(resultAsJsonDict['Inputs']), resultAsJsonDict['GlobalParameters']


class BatchExecution(BaseExecution):
    """ This class provides static methods to call AzureML services in batch mode"""


    @staticmethod
    def pushInputsToBlobStorage_and_CreateOutputsReferences(inputsDfDict: typing.Dict[str, pandas.DataFrame],
                                                            blob_service: BlockBlobService, blob_container: str,
                                                            blob_path_prefix: str = None, charset: str = None,
                                                            outputNames: typing.List[str] = []
                                                            ) -> \
        typing.Tuple[typing.Dict[str, typing.Dict[str, str]], typing.Dict[str, typing.Dict[str, str]]]:
        """
        Utility method to push all inputs from the provided dictionary into the selected blob storage on the cloud.
        Each input is an entry of the dictionary and should be a Dataframe.
        The inputs will be written to the blob using the provided charset.

        Files created on the blob storage will have a prefix generated from the current time, in order to
        quickly identify inputs pushed at the same time. For convenience, this prefix is provided as an output of this
        function so that outputs may be

        :param inputsDfDict:
        :param outputNames:
        :param blob_service:
        :param blob_container: the blob container name
        :param blob_path_prefix: the prefix to use for all blobs
        :param charset:
        :return: a tuple containing (1) a dictionary of "by reference" input descriptions
                and (2) a dictionary of "by reference" output descriptions
        """

        # 1- create unique blob naming prefix
        now = datetime.now()
        uniqueBlobNamePrefix = now.strftime("%Y-%m-%d_%H%M%S_%f")

        # 2- store INPUTS and retrieve references
        inputReferences = Collection_Converters.DfDict_to_BlobCsvRefDict(inputsDfDict, blob_service=blob_service,
                                                                         blob_container=blob_container,
                                                                         blob_path_prefix=blob_path_prefix,
                                                                         blob_name_prefix=uniqueBlobNamePrefix + '-input-',
                                                                         charset=charset)

        # 3- create OUTPUT references
        outputReferences = Collection_Converters.createBlobCsvRefDict(blob_names=outputNames, blob_service=blob_service,
                                                                      blob_container=blob_container,
                                                                      blob_path_prefix=blob_path_prefix,
                                                                      blob_name_prefix=uniqueBlobNamePrefix + '-output-')

        return inputReferences, outputReferences





    @staticmethod
    def createRequestJsonBody(inputReferences: typing.Dict[str, typing.Dict[str, str]]=None,
                              paramsDfOrDict=None,
                              outputReferences: typing.Dict[str, typing.Dict[str, str]]=None) -> str:
        """
        Helper method to create a JSON AzureML web service input in Batch mode, from 'by reference' inputs, and parameters as dataframe

        :param inputReferences: a dictionary containing input names and input references (each input reference is a dictionary)
        :param paramsDfOrDict: a dictionary of parameter names and values
        :param outputReferences: a dictionary containing output names and output references (each output reference is a dictionary)
        :return: a string representation of the request JSON body (not yet encoded in bytes)
        """

        # params
        if paramsDfOrDict is None:
            paramsDfOrDict = {}

        if isinstance(paramsDfOrDict, dict):
            params = paramsDfOrDict
        elif isinstance(paramsDfOrDict, pandas.DataFrame):
            params = Converters.ParamDf_to_ParamDict(paramsDfOrDict)
        else:
            raise TypeError(
                'paramsDfOrDict should be a dataframe or a dictionary, or None, found: ' + type(paramsDfOrDict))

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
    def readStatusOrResultJson(jsonJobStatusOrResult: str) -> typing.Dict[str, typing.Dict[str, str]]:
        """
        Reads the status or the result of an AzureML Batch job (asynchronous, by reference).
        Throws an error if the status is an error, or an empty result if the status is a

        :param jsonJobStatusOrResult:
        :return: the status as a dictionary, and throws an error if the job had an error
        """

        # first read the json as a dictionary
        resultAsJsonDict = json.loads(jsonJobStatusOrResult)

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
