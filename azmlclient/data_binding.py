from __future__ import print_function
import csv
import json
import sys
from datetime import datetime
from io import BytesIO   # for handling byte strings
from io import StringIO  # for handling unicode strings

try:  # python 3.5+
    from typing import Dict, Union, List, Any, Tuple
except ImportError:
    pass

import numpy as np
import pandas
import requests
from azure.storage.blob import BlockBlobService, ContentSettings


try:
    from csv import unix_dialect
except ImportError:
    class unix_dialect(csv.Dialect):
        """Describe the usual properties of Unix-generated CSV files."""
        delimiter = ','
        quotechar = '"'
        doublequote = True
        skipinitialspace = False
        lineterminator = '\n'
        quoting = csv.QUOTE_ALL
    csv.register_dialect("unix", unix_dialect)

if sys.version_info >= (3, 0):
    def create_dest_buffer_for_csv():
        return StringIO(newline='')

    def create_reading_buffer(value):
        return StringIO(value)
else:
    def create_dest_buffer_for_csv():
        return BytesIO()  # StringIO(newline='')

    def create_reading_buffer(value):
        return BytesIO(value)


def _check_not_none_and_typed(var, var_type=None, var_name=None):
    """
    Helper method to check that an object is not none and possibly f a certain type

    :param var: the object
    :param var_type: the type
    :param var_name: the name of the varioable to be used in error messages
    :return:
    """

    if var is None:
        if var_name is None:
            raise TypeError('Error, object should be non-None')
        else:
            raise TypeError('Error, object with name "' + var_name + '" should be non-None')
    elif var_type is not None:
        if not isinstance(var, var_type):
            if var_name is None:
                raise TypeError('Error, object should be a ' + var_type + ', found: ' + str(
                    type(var)))
            else:
                raise TypeError('Error, object with name "' + var_name + '" should be a ' + var_type + ', found: ' + str(
                    type(var)))
    return


class AzmlException(Exception):
    """
    Represents an AzureMl exception, built from an HTTP error body received from AzureML.
    """

    def __init__(self,
                 httpError  # type: requests.exceptions.HTTPError
                 ):

        # Try to decode the error body and print it
        # ----- old with urllib
        #jsonError = str(object=httpError.read(), encoding=httpError.headers.get_content_charset())
        # ----- new with requests
        jsonError = httpError.response.text
        errorAsDict = Converters.jsonstr_to_dict(jsonError)

        if 'error' in errorAsDict.keys():
            errorAsDict = errorAsDict['error']

            if 'code' in errorAsDict.keys() and 'message' in errorAsDict.keys() and 'details' in errorAsDict.keys():
                maincode = errorAsDict['code']
                mainmessage = errorAsDict['message']
                if isinstance(errorAsDict['details'], list) and len(errorAsDict['details']) == 1:
                    detailselement = errorAsDict['details'][0]
                    if 'code' in detailselement.keys() and 'message' in detailselement.keys():
                        super(AzmlException, self).__init__('Error [' + maincode + '][' + detailselement['code'] + ']: ' + mainmessage + '. ' + detailselement['message'])
                    else:
                        # fallback to main code and message
                        super(AzmlException, self).__init__('Error [' + maincode + ']: ' + mainmessage)
                else:
                    super(AzmlException, self).__init__('Error [' + maincode + ']: ' + mainmessage)

                self.maincode = maincode
                self.mainmessage = mainmessage

                # store dict for reference
                self.__errorAsDict = errorAsDict
            else:
                # noinspection PyTypeChecker
                raise ValueError(
                    'Unrecognized format for AzureML http error. JSON content is : ' + print(
                        errorAsDict))

        else:
            # noinspection PyTypeChecker
            raise ValueError('Unrecognized format for AzureML http error, no field named "error". JSON content is : ' + print(errorAsDict))



    def __str__(self):

        # if 'error' in self.__errorAsDict:
        #     # this is an azureML standard error
        #     if self.__errorAsDict['error']['code'] == 'LibraryExecutionError':
        #         if self.__errorAsDict['error']['details'][0]['code'] == 'TableSchemaColumnCountMismatch':
        #             return 'Dynamic schema validation is not supported in Request-Response mode, you should maybe use the BATCH response mode by setting useBatchMode to true in python'

        return json.dumps(self.__errorAsDict, indent=4)


class Converters(object):

    # @staticmethod
    # def httperror_to_azmlexception(http_error: urllib.error.HTTPError) -> AzmlException:
    #     return AzmlException(http_error)


    @staticmethod
    def df_to_csv(df,            # type: pandas.DataFrame
                  df_name=None,  # type: str
                  charset=None   # type: str
                  ):
        # type: (...) -> str
        """
        Converts the provided dataframe to a csv, to store it on blob storage for AzureML calls.
        WARNING: datetime columns are converted in ISO format but the milliseconds are ignored and set to zero.

        :param df:
        :param df_name:
        :return:
        """
        _check_not_none_and_typed(df, var_type=pandas.DataFrame, var_name=df_name)

        # TODO what about timezone detail if not present, will the %z be ok ?
        return df.to_csv(path_or_buf=None, sep=',', decimal='.', na_rep='', encoding=charset,
                         index=False, date_format='%Y-%m-%dT%H:%M:%S.000%z')

    @staticmethod
    def csv_to_df(csv_buffer_or_str_or_filepath,  # type: str
                  csv_name=None                   # type: str
                  ):
        # type: (...) -> pandas.DataFrame
        """
        Helper method to ensure consistent reading in particular for timezones and datetime parsing

        :param csv_buffer_or_str_or_filepath:
        :return:
        """
        _check_not_none_and_typed(csv_buffer_or_str_or_filepath, var_name=csv_name)

        # pandas does not accept string. create a buffer
        if isinstance(csv_buffer_or_str_or_filepath, str):
            csv_buffer_or_str_or_filepath = create_reading_buffer(csv_buffer_or_str_or_filepath)

        # read without parsing dates
        res = pandas.read_csv(csv_buffer_or_str_or_filepath, sep=',', decimal='.')  # infer_datetime_format=True, parse_dates=[0]

        # -- try to infer datetime columns
        convert_all_datetime_columns(res)

        # -- additionally we automatically configure the timezone as UTC
        datetimeColumns = [colName for colName, colType in res.dtypes.items() if is_datetime_dtype(colType)]
        for datetimeCol in datetimeColumns:
            # time is in ISO format, so the time column after import is UTC. We just have to declare it
            try:
                res[datetimeCol] = res[datetimeCol].dt.tz_localize(tz="UTC")
            except TypeError:
                res[datetimeCol] = res[datetimeCol].dt.tz_convert(tz="UTC")

        return res

    @staticmethod
    def df_to_azmltable(df,                    # type: pandas.DataFrame
                        is_azml_output=False,  # type: bool
                        df_name=None           # type: str
                        ):
        # type: (...) -> Dict[str, Union[str, Dict[str, List]]]
        """
        Converts the provided Dataframe to a dictionary in the same format than the JSON expected by AzureML in the
        Request-Response services. Note that contents are kept as is (values are not converted to string yet)

        :param df:
        :param df_name:
        :return:
        """
        _check_not_none_and_typed(df, var_type=pandas.DataFrame, var_name=df_name)

        if is_azml_output:
            # use this method recursively, in 'not output' mode
            return {'type': 'table', 'value': Converters.df_to_azmltable(df, df_name=df_name)}
        else:
            return {'ColumnNames': df.columns.values.tolist(),
                    # "ColumnTypes": [dtype_to_azmltyp(dt) for dt in df.dtypes], dont do it, no reliable mapping anyway
                    "Values": df.values.tolist()}

    @staticmethod
    def azmltable_to_df(azmltable_dict,        # type: Dict[str, Union[str, Dict[str, List]]]
                        is_azml_output=False,  # type: bool
                        table_name=None        # type: str
                        ):
        # type: (...) -> pandas.DataFrame
        """
        Converts an AzureML table (JSON-like dictionary) into a dataframe. Since two formats exist (one for inputs and
        one for outputs), there is a parameter you can use to specify which one to use.

        :param is_azml_output:
        :param table_name:
        :param first_col_is_datetime:
        :return:
        """
        _check_not_none_and_typed(azmltable_dict, var_type=dict, var_name=table_name)

        if is_azml_output:
            if 'type' in azmltable_dict.keys() and 'value' in azmltable_dict.keys():
                if azmltable_dict['type'] == 'table':
                    # use this method recursively, in 'not output' mode
                    # noinspection PyTypeChecker
                    return Converters.azmltable_to_df(azmltable_dict['value'], table_name=table_name)
                else:
                    raise ValueError('This method is able to read table objects, found type=' + str(azmltable_dict['type']))
            else:
                raise ValueError(
                    'object should be a dictionary with two fields "type" and "value", found: ' + str(
                        azmltable_dict.keys()) + ' for table object: ' + table_name)
        else:
            if 'ColumnNames' in azmltable_dict.keys() and 'Values' in azmltable_dict.keys():
                values = azmltable_dict['Values']
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
                    # -- for that we first dump in a buffer in a CSV format
                    buffer = create_dest_buffer_for_csv()
                    writer = csv.writer(buffer, dialect='unix')
                    writer.writerows([azmltable_dict['ColumnNames']])
                    writer.writerows(values)
                    # -- and then we parse with pandas
                    res = Converters.csv_to_df(create_reading_buffer(buffer.getvalue()))  # StringIO
                    buffer.close()

                else:
                    # empty dataframe
                    res = pandas.DataFrame(columns=azmltable_dict['ColumnNames'])
            else:
                raise ValueError(
                    'object should be a dictionary with two fields ColumnNames and Values, found: ' + str(
                        azmltable_dict.keys()) + ' for table object: ' + table_name)
            return res

    @staticmethod
    def jsonstr_to_azmltable(json_str  # type: str
                             ):
        # type: (...) -> Dict[str, Union[str, Dict[str, List]]]
        return Converters.jsonstr_to_dict(json_str)

    @staticmethod
    def azmltable_to_jsonstr(azmltable_dict  # type: Dict[str, Union[str, Dict[str, List]]]
                             ):
        # type: (...) -> str
        return Converters.dict_to_jsonstr(azmltable_dict)

    @staticmethod
    def paramdf_to_paramdict(params_df  # type: pandas.DataFrame
                             ):
        # type: (...) -> Dict[str, str]
        """
        Converts a parameter dataframe into a dictionary following the structure required for JSON conversion

        :param params_df: a dictionary of parameter names and values
        :return: a dictionary of parameter names and values
        """
        _check_not_none_and_typed(params_df, var_type=pandas.DataFrame, var_name='paramsDataframe')

        # params = {}
        # for paramName in paramsDataframe.columns.values:
        #     params[paramName] = paramsDataframe.at[0, paramName]
        # return params
        return {paramName: params_df.at[0, paramName] for paramName in params_df.columns.values}


    @staticmethod
    def paramdict_to_paramdf(paramsDict  # type: Dict[str, Any]
                             ):
        # type: (...) -> pandas.DataFrame
        """
        Converts a parameter dictionary into a parameter dataframe

        :param paramsDict:
        :return:
        """
        _check_not_none_and_typed(paramsDict, var_type=dict, var_name='paramsDict')

        return pandas.DataFrame(paramsDict, index=[0])


    @staticmethod
    def dict_to_jsonstr(dictObject
                        ):
        """
        Transforms a dictionary to a JSON string. Datetimes are converted using ISO format.

        :param dictObject:
        :return:
        """
        jsonBodyStr = json.dumps(dictObject, default=Converters.__json_serial)
        return jsonBodyStr

    @staticmethod
    def jsonstr_to_dict(jsonStr  # type: str
                        ):
        # type: (...) -> Dict[str, Any]
        """
        Creates a dictionary from a json string.

        :param jsonStr:
        :return:
        """
        return json.loads(jsonStr)

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


class ByReference_Converters(object):

    @staticmethod
    def _get_valid_blob_path_prefix(blob_path_prefix  # type: str
                                    ):
        #type: (...) -> str
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
    def _get_valid_blob_name_prefix(blob_name_prefix  # type: str
                                    ):
        # type: (...) -> str
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
    def _get_blob_service_connection_string(blob_service  # type: BlockBlobService
                                            ):
        # type: (...) -> str
        """
        Utilty method to get the connection string for a blob storage service (currently the BlockBlobService does
        not provide any method to do that)

        :param blob_service:
        :return:
        """
        _check_not_none_and_typed(blob_service, BlockBlobService, 'blob_service')

        return 'DefaultEndpointsProtocol=https;AccountName=' + blob_service.account_name + ';AccountKey=' + blob_service.account_key

    @staticmethod
    def create_blobcsvref(blob_service,           # type: BlockBlobService
                          blob_container,         # type: str
                          blob_name,              # type: str
                          blob_path_prefix=None,  # type: str
                          blob_name_prefix=None   # type: str
                          ):
        # type: (...) -> Tuple[Dict[str, str], str]
        """
        Utility method to create a reference to a blob, whether it exists or not

        :param blob_service:
        :param blob_container:
        :param blob_name:
        :param blob_path_prefix:
        :param blob_name_prefix:
        :return: a tuple. First element is the blob reference (a dict). Second element is the full blob name
        """
        _check_not_none_and_typed(blob_container, str, 'blob_container')
        _check_not_none_and_typed(blob_name, str, 'blob_name')

        # fix the blob name
        if blob_name.lower().endswith('.csv'):
            blob_name = blob_name[:-4]

        # validate blob service and get conection string
        connectionString = ByReference_Converters._get_blob_service_connection_string(blob_service)

        # check the blob path prefix, append a trailing slash if necessary
        blob_path_prefix = ByReference_Converters._get_valid_blob_path_prefix(blob_path_prefix)
        blob_name_prefix = ByReference_Converters._get_valid_blob_name_prefix(blob_name_prefix)

        blob_full_name = blob_path_prefix + blob_name_prefix + blob_name + '.csv'

        # output reference and full name
        return {'ConnectionString': connectionString,
                'RelativeLocation': blob_container + '/' + blob_path_prefix + blob_name_prefix + blob_name + '.csv'}, \
               blob_full_name

    @staticmethod
    def csv_to_blobcsvref(csv_str,                # type: str
                          blob_service,           # type: BlockBlobService
                          blob_container,         # type: str
                          blob_name,              # type: str
                          blob_path_prefix=None,  # type: str
                          blob_name_prefix=None,  # type: str
                          charset=None            # type: str
                          ):
        # type: (...) -> Dict[str, str]

        # setup the charset used for file encoding
        if charset is None:
            charset = 'utf-8'
        if charset != 'utf-8':
            print('Warning: blobs can be written in any charset but currently only utf-8 blobs may be read back into '
                  'dataframes. We recommend setting charset to None or utf-8 ')

        _check_not_none_and_typed(csv_str, str, 'csv_str')
        _check_not_none_and_typed(blob_name, str, 'blob_name')

        # 1- first create the references in order to check all params are ok
        blob_reference, blob_full_name = ByReference_Converters.create_blobcsvref(blob_service=blob_service,
                                                                                  blob_container=blob_container,
                                                                                  blob_path_prefix=blob_path_prefix,
                                                                                  blob_name_prefix=blob_name_prefix,
                                                                                  blob_name=blob_name)

        # -- push blob
        # noinspection PyTypeChecker
        blob_service.create_blob_from_stream(blob_container, blob_full_name, BytesIO(csv_str.encode(encoding=charset)),
                                             content_settings=ContentSettings(content_type='text.csv',
                                                                              content_encoding=charset))

        # ********** OLD method : with temporary files ***********
        # # 2- open a temporary file on this computer to write the csv
        # (fileDescriptor, filePath) = tempfile.mkstemp()
        # try:
        #     # 3- write the input to this file
        #     file = os.fdopen(fileDescriptor, mode='w', encoding=charset)
        #     file.write(csv_str)
        #     file.flush()
        #
        #     # 4- push the file into an uniquely named blob on the cloud
        #     blob_service.create_blob_from_path(blob_container, blob_full_name, filePath,
        #                                        content_settings=ContentSettings(content_type='text.csv', content_encoding=charset))
        #
        #     # 5- return reference
        #     return blob_reference
        #
        # except Exception as error:
        #     print('Error while writing input ' + blob_name + ' to blob storage')
        #     raise error
        #
        # finally:
        #     # Whatever the situation, close the input file and delete it
        #     try:
        #         os.close(fileDescriptor)
        #     finally:
        #         os.remove(filePath)

        return blob_reference

    @staticmethod
    def blobcsvref_to_csv(blob_reference,        # type: Dict
                          blob_name=None,        # type: str
                          encoding=None,         # type: str
                          requests_session=None  # type: requests.Session
                          ):
        """
        Reads a CSV referenced according to the format defined by AzureML, and transforms it into a Dataframe

        :param blob_reference:
        :param encoding:
        :param requests_session: an optional Session object that should be used for the HTTP communication
        :return:
        """
        _check_not_none_and_typed(blob_reference, var_type=dict, var_name=blob_name)

        if not(encoding is None or encoding=='utf-8'):
            raise ValueError('Unsupported encoding to retrieve blobs : ' + encoding)

        if ('ConnectionString' in blob_reference.keys()) and ('RelativeLocation' in blob_reference.keys()):

            # create the Blob storage client for this account
            blob_service = BlockBlobService(connection_string=blob_reference['ConnectionString'],
                                            request_session=requests_session)

            # find the container and blob path
            container, name = blob_reference['RelativeLocation'].split(sep='/', maxsplit=1)

            # retrieve it and convert
            # -- this works but is probably less optimized for big blobs that get chunked, than using streaming
            blob_string = blob_service.get_blob_to_text(blob_name=name, container_name=container)
            return blob_string.content

        else:
            raise ValueError('Blob reference is invalid: it should contain ConnectionString and RelativeLocation fields')

    @staticmethod
    def df_to_blobcsvref(df,                # type: pandas.DataFrame
                         blob_service,      # type: BlockBlobService
                         blob_container,    # type: str
                         blob_name,         # type: str
                         blob_path_prefix=None,  # type: str
                         blob_name_prefix=None,  # type: str
                         charset=None            # type: str
                         ):
        # type: (...) -> Dict[str, str]
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
        csv_str = Converters.df_to_csv(df, df_name=blob_name, charset=charset)

        # upload it
        return ByReference_Converters.csv_to_blobcsvref(csv_str, blob_service=blob_service,
                                                        blob_container=blob_container,
                                                        blob_path_prefix=blob_path_prefix,
                                                        blob_name_prefix=blob_name_prefix,
                                                        blob_name=blob_name, charset=charset)

    @staticmethod
    def blobcsvref_to_df(blob_reference,        # type: Dict
                         blob_name=None,        # type: str
                         encoding=None,         # type: str
                         requests_session=None  # type: requests.Session
                         ):
        """
        Reads a CSV blob referenced according to the format defined by AzureML, and transforms it into a Dataframe

        :param blob_reference:
        :param encoding:
        :param requests_session: an optional Session object that should be used for the HTTP communication
        :return:
        """

        # TODO copy the BlobCsvRef_to_Csv method here and handle the blob in streaming mode to be big blobs chunking-compliant.
        # However how to manage the buffer correctly, create the StringIO with correct encoding, and know the number of chunks
        # that should be read in pandas.read_csv ? A lot to dig here to get it right...
        #
        # from io import TextIOWrapper
        # contents = TextIOWrapper(buffer, encoding=charset, ...)
        # blob = blob_service.get_blob_to_stream(blob_name=name, container_name=container, encoding=charset, stream=contents)

        blob_content = ByReference_Converters.blobcsvref_to_csv(blob_reference, blob_name=blob_name, encoding=encoding,
                                                                requests_session=requests_session)

        if len(blob_content) > 0:
            return Converters.csv_to_df(StringIO(blob_content), blob_name)
        else:
            return pandas.DataFrame()


class Collection_Converters(object):

    @staticmethod
    def create_blob_csv_ref_dict(blob_service,           # type: BlockBlobService
                                 blob_container,         # type: str
                                 blob_names,             # type : List[str]
                                 blob_path_prefix=None,  # type: str
                                 blob_name_prefix=None   # type: str
                                 ):
        # type: (...) -> Dict[str, Dict[str, str]]
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
        return {blobName: ByReference_Converters.create_blobcsvref(blob_service, blob_container, blobName,
                                                                   blob_path_prefix=blob_path_prefix,
                                                                   blob_name_prefix=blob_name_prefix)[0]
                for blobName in blob_names}

    @staticmethod
    def dfdict_to_csvdict(dataframesDict,   # type: Dict[str, pandas.DataFrame]
                          charset=None      # type: str
                          ):
        # type: (...) -> Dict[str, str]
        """
        Helper method to create CSVs compliant with AzureML web service BATCH inputs, from a dictionary of input dataframes

        :param dataframesDict: a dictionary containing input names and input content (each input content is a dataframe)
        :return: a dictionary containing the string representations of the Csv inputs to store on the blob storage
        """
        _check_not_none_and_typed(dataframesDict, var_type=dict, var_name='dataframesDict')

        return {inputName: Converters.df_to_csv(inputDf, df_name=inputName, charset=charset) for inputName, inputDf in dataframesDict.items()}


    @staticmethod
    def csvdict_to_dfdict(csvsDict  # type: Dict[str, str]
                          ):
        # type: (...) -> Dict[str, pandas.DataFrame]
        """
        Helper method to read CSVs compliant with AzureML web service BATCH inputs/outputs, into a dictionary of Dataframes

        :param csvsDict:
        :return:
        """
        _check_not_none_and_typed(csvsDict, var_type=dict, var_name='csvsDict')

        return {inputName: Converters.csv_to_df(inputCsv, csv_name=inputName) for inputName, inputCsv in csvsDict.items()}


    @staticmethod
    def dfdict_to_azmltablesdict(dataframesDict  # type: Dict[str, pandas.DataFrame]
                                 ):
        # type: (...) -> Dict[str, Dict[str, Union[str, Dict[str, List]]]]
        """
        Converts a dictionary of dataframes into a dictionary of dictionaries following the structure
        required for AzureML JSON conversion

        :param dataframesDict: a dictionary containing input names and input content (each input content is a dataframe)
        :return: a dictionary of tables represented as dictionaries
        """
        _check_not_none_and_typed(dataframesDict, var_type=dict, var_name='dataframesDict')

        # resultsDict = {}
        # for dfName, df in dataframesDict.items():
        #     resultsDict[dfName] = Converters.Df_to_AzmlTable(df, dfName)
        # return resultsDict

        return {dfName: Converters.df_to_azmltable(df, df_name=dfName) for dfName, df in dataframesDict.items()}

    @staticmethod
    def azmltablesdict_to_dfdict(azmlTablesDict,        # type: Dict[str, Dict[str, Union[str, Dict[str, List]]]]
                                 isAzureMlOutput=False  # type: bool
                                 ):
        # type: (...) -> Dict[str, pandas.DataFrame]

        _check_not_none_and_typed(azmlTablesDict, var_type=dict, var_name='azmlTablesDict')

        return {input_name: Converters.azmltable_to_df(dict_table,
                                                       is_azml_output=isAzureMlOutput, table_name=input_name)
                for input_name, dict_table in azmlTablesDict.items()}

    @staticmethod
    def blobcsvrefdict_to_csvdict(blobcsvReferences,     # type: Dict[str, Dict[str, str]]
                                  charset=None,          # type: str
                                  requests_session=None  # type: requests.Session
                                  ):
        # type: (...) -> Dict[str, str]
        """

        :param blobcsvReferences:
        :param charset:
        :param requests_session: an optional Session object that should be used for the HTTP communication
        :return:
        """

        _check_not_none_and_typed(blobcsvReferences, dict, 'blobcsvReferences')

        return {blobName: ByReference_Converters.blobcsvref_to_csv(csvBlobRef, encoding=charset, blob_name=blobName,
                                                                   requests_session=requests_session)
                for blobName, csvBlobRef in blobcsvReferences.items()}


    @staticmethod
    def csvdict_to_blobcsvrefdict(csvsDict,               # type: Dict[str, str]
                                  blob_service,           # type: BlockBlobService
                                  blob_container,         # type: str
                                  blob_path_prefix=None,  # type: str
                                  blob_name_prefix=None,  # type: str
                                  charset=None            # type: str
                                  ):
        # type: (...) -> Dict[str, Dict[str, str]]
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

        return {blobName: ByReference_Converters.csv_to_blobcsvref(csvStr, blob_service=blob_service,
                                                                   blob_container=blob_container,
                                                                   blob_path_prefix=blob_path_prefix,
                                                                   blob_name_prefix=blob_name_prefix,
                                                                   blob_name=blobName, charset=charset)
                for blobName, csvStr in csvsDict.items()}



    @staticmethod
    def blobcsvrefdict_to_dfdict(blobReferences,        # type: Dict[str, Dict[str, str]]
                                 charset=None,          # type: str
                                 requests_session=None  # type: requests.Session
                                 ):
        # type: (...) -> Dict[str, pandas.DataFrame]
        """
        Reads Blob references, for example responses from an AzureMl Batch web service call, into a dictionary of
        pandas dataframe

        :param blobReferences: the json output description by reference for each output
        :param charset:
        :param requests_session: an optional Session object that should be used for the HTTP communication
        :return: the dictionary of corresponding dataframes mapped to the output names
        """
        _check_not_none_and_typed(blobReferences, dict, 'blobReferences')

        return {blobName: ByReference_Converters.blobcsvref_to_df(csvBlobRef, encoding=charset, blob_name=blobName,
                                                                  requests_session=requests_session)
                for blobName, csvBlobRef in blobReferences.items()}

    @staticmethod
    def dfdict_to_blobcsvrefdict(dataframesDict,         # type: Dict[str, pandas.DataFrame]
                                 blob_service,           # type: BlockBlobService
                                 blob_container,         # type: str
                                 blob_path_prefix=None,  # type: str
                                 blob_name_prefix=None,  # type: str
                                 charset=None            # type: str
                                 ):
        # type: (...) -> Dict[str, Dict[str, str]]

        _check_not_none_and_typed(dataframesDict, dict, 'dataframesDict')

        return {blobName: ByReference_Converters.df_to_blobcsvref(csvStr, blob_service=blob_service,
                                                                  blob_container=blob_container,
                                                                  blob_path_prefix=blob_path_prefix,
                                                                  blob_name_prefix=blob_name_prefix,
                                                                  blob_name=blobName, charset=charset)
                for blobName, csvStr in dataframesDict.items()}


def convert_all_datetime_columns(df):
    """
    Utility method to try to convert all datetime columns in the provided dataframe, inplace.
    Note that only columns with dtype 'object' are considered as possible candidates.

    :param df:
    :return:
    """
    objColumns = [colName for colName, colType in df.dtypes.items() if colType == np.dtype('O')]
    for obj_col_name in objColumns:
        try:
            df[obj_col_name] = pandas.to_datetime(df[obj_col_name])
        except Exception:
            # silently escape, do not convert
            pass


def is_datetime_dtype(dtyp):
    return np.issubdtype(dtyp.base, np.dtype(np.datetime64))
