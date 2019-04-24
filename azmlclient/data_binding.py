from __future__ import print_function
import csv
import json
import sys
from collections import OrderedDict
from datetime import datetime
from io import BytesIO   # for handling byte strings
from io import StringIO  # for handling unicode strings

try:  # python 3.5+
    from typing import Dict, Union, List, Any, Tuple

    SwaggerModeAzmlTable = List[Dict[str, Any]]
    NonSwaggerModeAzmlTable = Dict[str, Union[List[str], List[List[Any]]]]
    AzmlTable = Union[SwaggerModeAzmlTable, NonSwaggerModeAzmlTable]
    AzmlOutputTable = Dict[str, Union[str, AzmlTable]]

    AzmlBlobTable = Dict[str, str]
except ImportError:
    pass

import numpy as np
import pandas
import requests
from valid8 import validate

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


class AzmlException(Exception):
    """
    Represents an AzureMl exception, built from an HTTP error body received from AzureML.
    Once constructed from an HTTPError, the error details appear in the exception fields.
    """

    def __init__(self,
                 http_error  # type: requests.exceptions.HTTPError
                 ):
        """
        Constructor from an http error received from `requests`.

        :param http_error:
        """
        # extract error contents from http json body
        json_error = http_error.response.text
        error_as_dict = Converters.json_to_azmltable(json_error)

        # main error elements
        try:
            self.error_dict = error_as_dict['error']
            self.error_code = self.error_dict['code']
            self.error_message = self.error_dict['message']
            self.details = error_as_dict['details']
        except KeyError:
            raise ValueError("Unrecognized format for AzureML http error. JSON content is :\n %s" % error_as_dict)

        # create the message based on contents
        try:
            details_dict = error_as_dict['details'][0]
            details_code = details_dict['code']
            details_msg = details_dict['message']
        except (IndexError, KeyError):
            msg = 'Error [%s]: %s' % (self.error_code, self.error_message)
        else:
            msg = 'Error [%s][%s]: %s. %s' % (self.error_code, details_code, self.error_message, details_msg)

        # finally call super
        super(AzmlException, self).__init__(msg)

    def __str__(self):
        # if 'error' in self.__errorAsDict:
        #     # this is an azureML standard error
        #     if self.__errorAsDict['error']['code'] == 'LibraryExecutionError':
        #         if self.__errorAsDict['error']['details'][0]['code'] == 'TableSchemaColumnCountMismatch':
        #             return 'Dynamic schema validation is not supported in Request-Response mode, you should maybe
        #             use the BATCH response mode by setting useBatchMode to true in python'
        return json.dumps(self.error_dict, indent=4)


class Converters(object):
    """
    Static class containing all converters for single objects
    """

    @staticmethod
    def df_to_csv(df,            # type: pandas.DataFrame
                  df_name=None,  # type: str
                  charset=None   # type: str
                  ):
        # type: (...) -> str
        """
        Converts the provided DataFrame to a csv, typically to store it on blob storage for Batch AzureML calls.
        WARNING: datetime columns are converted in ISO format but the milliseconds are ignored and set to zero.

        :param df:
        :param df_name: the name of the dataframe, for error messages
        :param charset: the charset to use for encoding
        :return:
        """
        validate(df_name, df, instance_of=pandas.DataFrame)

        # TODO what about timezone detail if not present, will the %z be ok ?
        return df.to_csv(path_or_buf=None, sep=',', decimal='.', na_rep='', encoding=charset,
                         index=False, date_format='%Y-%m-%dT%H:%M:%S.000%z')

    @staticmethod
    def csv_to_df(csv_buffer_or_str_or_filepath,  # type: Union[str, StringIO, BytesIO]
                  csv_name=None                   # type: str
                  ):
        # type: (...) -> pandas.DataFrame
        """
        Converts the provided csv to a DatFrame, typically to read it from blob storage for Batch AzureML calls.
        Helper method to ensure consistent reading in particular for timezones and datetime parsing

        :param csv_buffer_or_str_or_filepath:
        :param csv_name: the name of the dataframe, for error messages
        :return:
        """
        validate(csv_name, csv_buffer_or_str_or_filepath)

        # pandas does not accept string. create a buffer
        if isinstance(csv_buffer_or_str_or_filepath, str):
            csv_buffer_or_str_or_filepath = create_reading_buffer(csv_buffer_or_str_or_filepath)

        # read without parsing dates
        res = pandas.read_csv(csv_buffer_or_str_or_filepath, sep=',', decimal='.')  # infer_datetime_format=True, parse_dates=[0]

        # -- try to infer datetime columns
        convert_all_datetime_columns(res)

        # -- additionally we automatically configure the timezone as UTC
        localize_all_datetime_columns(res)

        return res

    @staticmethod
    def df_to_azmltable(df,                         # type: pandas.DataFrame
                        table_name=None,            # type: str
                        swagger=False,              # type: bool
                        mimic_azml_output = False,  # type: bool
                        ):
        # type: (...) -> Union[AzmlTable, AzmlOutputTable]
        """
        Converts the provided DataFrame to a dictionary or list in the same format than the JSON expected by AzureML in
        the Request-Response services. Note that contents are kept as is (values are not converted to string yet)

        :param df: the dataframe to convert
        :param table_name: the table name for error messages
        :param swagger: a boolean indicating if the swagger format should be used (more verbose). Default: False
        :param mimic_azml_output: set this to True if the result should be wrapped in a dictionary like azureml outputs.
            This is typically needed if you wish to mimic an AzureML web service's behaviour, for a mock web server.
        :return:
        """
        validate(table_name, df, instance_of=pandas.DataFrame)

        if mimic_azml_output:
            # use this method recursively, in 'not output' mode
            return {'type': 'table', 'value': Converters.df_to_azmltable(df, table_name=table_name, swagger=swagger)}
        else:
            col_names = df.columns.values.tolist()
            if swagger:
                # swagger mode: the table is a list of object rows
                return [OrderedDict([(col_name, df[col_name].iloc[i]) for col_name in col_names])
                        for i in range(df.shape[0])]
            else:
                # non-swagger mode: the columns and values are separate attributes.
                #
                # "ColumnTypes": [dtype_to_azmltyp(dt) for dt in df.dtypes],
                # --> dont do type conversion, azureml type mapping does not seem to be reliable enough.
                return {'ColumnNames': col_names,
                        "Values": df.values.tolist()}

    @staticmethod
    def azmltable_to_df(azmltable,             # type: Union[AzmlTable, AzmlOutputTable]
                        is_azml_output=False,  # type: bool
                        table_name=None        # type: str
                        ):
        # type: (...) -> pandas.DataFrame
        """
        Converts a parsed AzureML table (JSON-like dictionary or list obtained from parsing the json body) into a
        dataframe. Since two formats exist (one for inputs and one for outputs), there is a parameter you can use to
        specify which one to use.

        :param azmltable: the AzureML table to convert
        :param is_azml_output: set this to True if the `azmltable` was received from an actual AzureML web service.
            Indeed in this case the table is usually wrapped in a dictionary that needs to be unwrapped.
        :param table_name: the table name for error messages
        :return:
        """
        validate(table_name, azmltable, instance_of=(list, dict))

        if is_azml_output:
            if 'type' in azmltable.keys() and 'value' in azmltable.keys():
                if azmltable['type'] == 'table':
                    # use this method recursively, in 'not output' mode
                    # noinspection PyTypeChecker
                    return Converters.azmltable_to_df(azmltable['value'], table_name=table_name)
                else:
                    raise ValueError("This method is able to read table objects, found type=%s" % azmltable['type'])
            else:
                raise ValueError("object should be a dictionary with two fields 'type' and 'value', found: %s for "
                                 "table object: %s" % (azmltable.keys(), table_name))
        else:
            if isinstance(azmltable, list):
                # swagger format
                values = []
                if len(azmltable) > 0:
                    colnames = list(azmltable[0].keys())
                    for i, row in enumerate(azmltable):
                        try:
                            rowvals = [row[k] for k in colnames]
                            values.append(rowvals)
                            if len(row) > len(colnames):
                                new_cols = set(row.keys()) - set(colnames)
                                raise ValueError("Columns are present in row #%s but not in the first row: "
                                                 "%s" % (i + 1, new_cols))
                        except KeyError as e:
                            raise ValueError("A column is missing in row #%s: %s" % (i + 1, e))
                else:
                    colnames = []

            elif 'ColumnNames' in azmltable.keys() and 'Values' in azmltable.keys():
                # non-swagger format
                values = azmltable['Values']
                colnames = azmltable['ColumnNames']
            else:
                raise ValueError("object should be a list or a dictionary with two fields ColumnNames and Values, "
                                 "found: %s for table object: %s" % (azmltable.keys(), table_name))

            if len(values) > 0:
                # # create dataframe manually
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

                # Easier: use pandas csv parser to infer most of the types
                # -- for that we first dump in a buffer in a CSV format
                buffer = create_dest_buffer_for_csv()
                writer = csv.writer(buffer, dialect='unix')
                writer.writerows([colnames])
                writer.writerows(values)
                # -- and then we parse with pandas
                res = Converters.csv_to_df(create_reading_buffer(buffer.getvalue()))  # StringIO
                buffer.close()

            else:
                # empty dataframe
                res = pandas.DataFrame(columns=colnames)

            return res

    @staticmethod
    def params_df_to_params_dict(params_df  # type: pandas.DataFrame
                                 ):
        # type: (...) -> Dict[str, str]
        """
        Converts a parameters DataFrame into a dictionary following the structure required for JSON conversion

        :param params_df: a dictionary of parameter names and values
        :return: a dictionary of parameter names and values
        """
        validate('params_df', params_df, instance_of=pandas.DataFrame)
        return {param_name: params_df.at[0, param_name] for param_name in params_df.columns.values}

    @staticmethod
    def params_dict_to_params_df(params_dict  # type: Dict[str, Any]
                                 ):
        # type: (...) -> pandas.DataFrame
        """
        Converts a parameter dictionary into a parameter dataframe

        :param params_dict:
        :return:
        """
        validate('params_dict', params_dict, instance_of=dict)

        # create a single-row DataFrame
        return pandas.DataFrame(params_dict, index=[0])

    @staticmethod
    def azmltable_to_json(azmltable  # type: Union[AzmlTable, AzmlOutputTable]
                          ):
        # type: (...) -> str
        """
        Transforms an AzureML table to a JSON string.
        Datetimes are converted using ISO format.

        :param azmltable:
        :return:
        """
        # dump using our custom serializer so that types are supported by AzureML
        return json.dumps(azmltable, default=Converters.azml_json_serializer)

    @staticmethod
    def json_to_azmltable(json_str  # type: str
                          ):
        # type: (...) -> Union[AzmlTable, AzmlOutputTable]
        """
        Creates an AzureML table from a json string.

        :param json_str:
        :return:
        """
        # load but keep order: use an ordered dict
        return json.loads(json_str, object_pairs_hook=OrderedDict)

    @staticmethod
    def azml_json_serializer(obj):
        """
        JSON custom serializer for objects not serializable by default json code

        :param obj:
        :return:
        """
        if isinstance(obj, np.integer):
            # since numpy ints are also bools, do ints first
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


class BlobConverters(object):
    """
    Static class containing all Azure blob storage-related "converters".
    Note that some converters (df_to_blob_ref, csv_to_blob_ref) actually *upload* data to an actual blob storage.
    """

    @staticmethod
    def csv_to_blob_ref(csv_str,                # type: str
                        blob_service,           # type: BlockBlobService
                        blob_container,         # type: str
                        blob_name,              # type: str
                        blob_path_prefix=None,  # type: str
                        charset=None            # type: str
                        ):
        # type: (...) -> AzmlBlobTable
        """
        Uploads the provided CSV to the selected Blob Storage service, and returns a reference to the created blob in
        case of success.

        :param csv_str:
        :param blob_service: the BlockBlobService to use, defining the connection string
        :param blob_container: the name of the blob storage container to use. This is the "root folder" in azure blob
            storage wording.
        :param blob_name: the "file name" of the blob, ending with .csv or not (in which case the .csv suffix will be
            appended)
        :param blob_path_prefix: an optional folder prefix that will be used to store your blob inside the container.
            For example "path/to/my/"
        :param charset:
        :return:
        """
        # setup the charset used for file encoding
        if charset is None:
            charset = 'utf-8'
        elif charset != 'utf-8':
            print("Warning: blobs can be written in any charset but currently only utf-8 blobs may be read back into "
                  "dataframes. We recommend setting charset to None or utf-8 ")

        # validate inputs (the only one that is not validated below)
        validate('csv_str', csv_str, instance_of=str)

        # 1- first create the references in order to check all params are ok
        blob_reference, blob_full_name = BlobConverters.create_blob_ref(blob_service=blob_service,
                                                                        blob_container=blob_container,
                                                                        blob_path_prefix=blob_path_prefix,
                                                                        blob_name=blob_name)

        # -- push blob
        blob_stream = BytesIO(csv_str.encode(encoding=charset))
        # noinspection PyTypeChecker
        blob_service.create_blob_from_stream(blob_container, blob_full_name, blob_stream,
                                             content_settings=ContentSettings(content_type='text.csv',
                                                                              content_encoding=charset))
        # (For old method with temporary files: see git history)

        return blob_reference

    @staticmethod
    def blob_ref_to_csv(blob_reference,        # type: AzmlBlobTable
                        blob_name=None,        # type: str
                        encoding=None,         # type: str
                        requests_session=None  # type: requests.Session
                        ):
        """
        Reads a CSV stored in a Blob Storage and referenced according to the format defined by AzureML, and transforms
        it into a DataFrame.

        :param blob_reference: a (azureml json-like) dictionary representing a table stored as a csv in a blob storage.
        :param blob_name: blob name for error messages
        :param encoding: an optional encoding to use to read the blob
        :param requests_session: an optional Session object that should be used for the HTTP communication
        :return:
        """
        validate(blob_name, blob_reference, instance_of=dict)

        if encoding is not None and encoding != 'utf-8':
            raise ValueError("Unsupported encoding to retrieve blobs : %s" % encoding)

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
    def df_to_blob_ref(df,                     # type: pandas.DataFrame
                       blob_service,           # type: BlockBlobService
                       blob_container,         # type: str
                       blob_name,              # type: str
                       blob_path_prefix=None,  # type: str
                       charset=None            # type: str
                       ):
        # type: (...) -> Dict[str, str]
        """
        Uploads the provided DataFrame to the selected Blob Storage service as a CSV file blob, and returns a reference
        to the created blob in case of success.

        :param df:
        :param blob_service: the BlockBlobService to use, defining the connection string
        :param blob_container: the name of the blob storage container to use. This is the "root folder" in azure blob
            storage wording.
        :param blob_name: the "file name" of the blob, ending with .csv or not (in which case the .csv suffix will be
            appended)
        :param blob_path_prefix: an optional folder prefix that will be used to store your blob inside the container.
            For example "path/to/my/"
        :param charset: the charset to use to encode the blob (default and recommended: 'utf-8')
        :return:
        """

        # create the csv
        csv_str = Converters.df_to_csv(df, df_name=blob_name, charset=charset)

        # upload it
        return BlobConverters.csv_to_blob_ref(csv_str, blob_service=blob_service,
                                              blob_container=blob_container,
                                              blob_path_prefix=blob_path_prefix,
                                              blob_name=blob_name, charset=charset)

    @staticmethod
    def blob_ref_to_df(blob_reference,        # type: AzmlBlobTable
                       blob_name=None,        # type: str
                       encoding=None,         # type: str
                       requests_session=None  # type: requests.Session
                       ):
        """
        Reads a CSV blob referenced according to the format defined by AzureML, and transforms it into a DataFrame

        :param blob_reference: a (azureml json-like) dictionary representing a table stored as a csv in a blob storage.
        :param blob_name: blob name for error messages
        :param encoding: an optional encoding to use to read the blob
        :param requests_session: an optional Session object that should be used for the HTTP communication
        :return:
        """
        # TODO copy the blob_ref_to_csv method here and handle the blob in streaming mode to be big blobs
        #  chunking-compliant. However how to manage the buffer correctly, create the StringIO with correct encoding,
        #  and know the number of chunks that should be read in pandas.read_csv ? A lot to dig here to get it right...
        #
        # from io import TextIOWrapper
        # contents = TextIOWrapper(buffer, encoding=charset, ...)
        # blob = blob_service.get_blob_to_stream(blob_name=name, container_name=container, encoding=charset,
        #                                        stream=contents)

        blob_content = BlobConverters.blob_ref_to_csv(blob_reference, blob_name=blob_name, encoding=encoding,
                                                      requests_session=requests_session)

        if len(blob_content) > 0:
            # convert to dataframe
            return Converters.csv_to_df(StringIO(blob_content), blob_name)
        else:
            # empty blob > empty dataframe
            return pandas.DataFrame()

    @staticmethod
    def create_blob_ref(blob_service,  # type: BlockBlobService
                        blob_container,  # type: str
                        blob_name,  # type: str
                        blob_path_prefix=None,  # type: str
                        ):
        # type: (...) -> Tuple[Dict[str, str], str]
        """
        Creates a reference in the AzureML format, to a csv blob stored on Azure Blob Storage, whether it exists or not.
        The blob name can end with '.csv' or not, the code handles both.

        :param blob_service: the BlockBlobService to use, defining the connection string
        :param blob_container: the name of the blob storage container to use. This is the "root folder" in azure blob
            storage wording.
        :param blob_name: the "file name" of the blob, ending with .csv or not (in which case the .csv suffix will be
            appended)
        :param blob_path_prefix: an optional folder prefix that will be used to store your blob inside the container.
            For example "path/to/my/"
        :return: a tuple. First element is the AzureML blob reference (a dict). Second element is the full blob name
        """
        # validate input (blob_service and blob_path_prefix are done below)
        validate('blob_container', blob_container, instance_of=str)
        validate('blob_name', blob_name, instance_of=str)

        # fix the blob name
        if blob_name.lower().endswith('.csv'):
            blob_name = blob_name[:-4]

        # validate blob service and get connection string
        connection_str = BlobConverters._get_blob_service_connection_string(blob_service)

        # check the blob path prefix, append a trailing slash if necessary
        blob_path_prefix = BlobConverters._get_valid_blob_path_prefix(blob_path_prefix)

        # output reference and full name
        blob_full_name = '%s%s.csv' % (blob_path_prefix, blob_name)
        relative_location = "%s/%s" % (blob_container, blob_full_name)
        output_ref = {'ConnectionString': connection_str,
                      'RelativeLocation': relative_location}

        return output_ref, blob_full_name

    @staticmethod
    def _get_valid_blob_path_prefix(blob_path_prefix  # type: str
                                    ):
        # type: (...) -> str
        """
        Utility method to get a valid blob path prefix from a provided one. A trailing slash is added if non-empty

        :param blob_path_prefix:
        :return:
        """
        validate('blob_path_prefix', blob_path_prefix, instance_of=str, enforce_not_none=False)

        if blob_path_prefix is None:
            blob_path_prefix = ''
        elif isinstance(blob_path_prefix, str):
            if len(blob_path_prefix) > 0 and not blob_path_prefix.endswith('/'):
                blob_path_prefix = blob_path_prefix + '/'
        else:
            raise TypeError("Blob path prefix should be a valid string or not be provided (default is empty string)")

        return blob_path_prefix

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
        validate('blob_service', blob_service, instance_of=BlockBlobService)

        return "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s" \
               "" % (blob_service.account_name, blob_service.account_key)


class CollectionConverters(object):
    """
    Helper methods to convert collection of objects. Note that as for `BlobConverters`, all blob-related methods
    actually *upload* blobs to storage services, so this is more than just conversion.

    The reason why this class is provided is to add some handy validation and prefixing capabilities for collections.
    """

    @staticmethod
    def create_blob_refs(blob_service,           # type: BlockBlobService
                         blob_container,         # type: str
                         blob_names,             # type: List[str]
                         blob_path_prefix=None,  # type: str
                         blob_name_prefix=None   # type: str
                         ):
        # type: (...) -> Dict[str, AzmlBlobTable]
        """
        Utility method to create one or several blob references on the same container on the same blob storage service.

        :param blob_service:
        :param blob_container:
        :param blob_names:
        :param blob_path_prefix: optional prefix to the blob names
        :param blob_name_prefix:
        :return:
        """
        validate('blob_names', blob_names, instance_of=list)
        if blob_name_prefix is None:
            blob_name_prefix = ""
        else:
            validate('blob_name_prefix', blob_name_prefix, instance_of=str)

        # convert all and return in a dict
        return {blob_name: BlobConverters.create_blob_ref(blob_service, blob_container, blob_name_prefix + blob_name,
                                                          blob_path_prefix=blob_path_prefix)[0]
                for blob_name in blob_names}

    @staticmethod
    def dfdict_to_csvdict(dfs,           # type: Dict[str, pandas.DataFrame]
                          charset=None   # type: str
                          ):
        # type: (...) -> Dict[str, str]
        """
        Converts each of the DataFrames in the provided dictionary to a csv, typically to store it on blob storage for
        Batch AzureML calls. All CSV are returned in a dictoinary with the same keys.

        WARNING: datetime columns are converted in ISO format but the milliseconds are ignored and set to zero.
        See `Converters.df_to_csv` for details

        :param dfs: a dictionary containing input names and input content (each input content is a dataframe)
        :param charset: the charset to use for csv encoding
        :return: a dictionary containing the string representations of the Csv inputs to store on the blob storage
        """
        validate('dfs', dfs, instance_of=dict)

        return {input_name: Converters.df_to_csv(inputDf, df_name=input_name, charset=charset)
                for input_name, inputDf in dfs.items()}

    @staticmethod
    def csvdict_to_dfdict(csv_dict  # type: Dict[str, str]
                          ):
        # type: (...) -> Dict[str, pandas.DataFrame]
        """
        Helper method to read CSVs compliant with AzureML web service BATCH inputs/outputs, into a dictionary of Dataframes

        :param csv_dict:
        :return:
        """
        validate('csv_dict', csv_dict, instance_of=dict)

        return {input_name: Converters.csv_to_df(inputCsv, csv_name=input_name)
                for input_name, inputCsv in csv_dict.items()}

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
        validate('dataframesDict', dataframesDict, instance_of=dict)

        # resultsDict = {}
        # for dfName, df in dataframesDict.items():
        #     resultsDict[dfName] = Converters.Df_to_AzmlTable(df, dfName)
        # return resultsDict

        return {dfName: Converters.df_to_azmltable(df, table_name=dfName) for dfName, df in dataframesDict.items()}

    @staticmethod
    def azmltablesdict_to_dfdict(azmlTablesDict,        # type: Dict[str, Dict[str, Union[str, Dict[str, List]]]]
                                 isAzureMlOutput=False  # type: bool
                                 ):
        # type: (...) -> Dict[str, pandas.DataFrame]

        validate('azmlTablesDict', azmlTablesDict, instance_of=dict)

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

        validate('blobcsvReferences', blobcsvReferences, instance_of=dict)

        return {blobName: BlobConverters.blob_ref_to_csv(csvBlobRef, encoding=charset, blob_name=blobName,
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

        validate('csvsDict', csvsDict, instance_of=dict)
        if blob_name_prefix is None:
            blob_name_prefix = ""
        else:
            validate('blob_name_prefix', blob_name_prefix, instance_of=str)

        return {blobName: BlobConverters.csv_to_blob_ref(csvStr, blob_service=blob_service,
                                                         blob_container=blob_container,
                                                         blob_path_prefix=blob_path_prefix,
                                                         blob_name=blob_name_prefix + blobName, charset=charset)
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
        validate('blobReferences', blobReferences, instance_of=dict)

        return {blobName: BlobConverters.blob_ref_to_df(csvBlobRef, encoding=charset, blob_name=blobName,
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

        validate('dataframesDict', dataframesDict, instance_of=dict)

        return {blobName: BlobConverters.df_to_blob_ref(csvStr, blob_service=blob_service,
                                                        blob_container=blob_container,
                                                        blob_path_prefix=blob_path_prefix,
                                                        blob_name=blob_name_prefix + blobName, charset=charset)
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


def localize_all_datetime_columns(df):
    """
    Localizes all datetime columns in df, inplace.
    :param df:
    :return:
    """
    datetime_cols = [colName for colName, colType in df.dtypes.items() if is_datetime_dtype(colType)]
    for datetime_col in datetime_cols:
        # time is in ISO format, so the time column after import is UTC. We just have to declare it
        try:
            df[datetime_col] = df[datetime_col].dt.tz_localize(tz="UTC")
        except TypeError:
            df[datetime_col] = df[datetime_col].dt.tz_convert(tz="UTC")


def is_datetime_dtype(dtyp):
    """
    Returns True if the given dtype is a datetime dtype
    :param dtyp:
    :return:
    """
    # return np.issubdtype(dtyp.base, np.dtype(np.datetime64))  -> does not work for int64
    return dtyp.kind == 'M'
