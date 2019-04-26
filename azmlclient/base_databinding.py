from __future__ import print_function
import csv
import json
import sys
from collections import OrderedDict
from datetime import datetime
from io import BytesIO   # to handle byte strings
from io import StringIO  # to handle unicode strings

try:  # python 3.5+
    from typing import Dict, Union, List, Any, Tuple

    # a few predefined type hints
    SwaggerModeAzmlTable = List[Dict[str, Any]]
    NonSwaggerModeAzmlTable = Dict[str, Union[List[str], List[List[Any]]]]
    AzmlTable = Union[SwaggerModeAzmlTable, NonSwaggerModeAzmlTable]
    AzmlOutputTable = Dict[str, Union[str, AzmlTable]]
except ImportError:
    pass

import numpy as np
import pandas
import requests
from valid8 import validate


try:
    from csv import unix_dialect
except ImportError:
    # noinspection PyPep8Naming,SpellCheckingInspection
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
        error_as_dict = json_to_azmltable(json_error)

        # main error elements
        try:
            self.error_dict = error_as_dict['error']
            # noinspection PyTypeChecker
            self.error_code = self.error_dict['code']
            # noinspection PyTypeChecker
            self.error_message = self.error_dict['message']
            self.details = error_as_dict['details']
        except KeyError:
            raise ValueError("Unrecognized format for AzureML http error. JSON content is :\n %s" % error_as_dict)

        # create the message based on contents
        try:
            details_dict = error_as_dict['details'][0]
            # noinspection PyTypeChecker
            details_code = details_dict['code']
            # noinspection PyTypeChecker
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


def df_to_csv(df,            # type: pandas.DataFrame
              df_name=None,  # type: str
              charset=None   # type: str
              ):
    # type: (...) -> str
    """
    Converts the provided DataFrame to a csv, typically to store it on blob storage for Batch AzureML calls.
    WARNING: datetime columns are converted in ISO format but the milliseconds are ignored and set to zero.

    :param df:
    :param df_name: the name of the DataFrame, for error messages
    :param charset: the charset to use for encoding
    :return:
    """
    validate(df_name, df, instance_of=pandas.DataFrame)

    # TODO what about timezone detail if not present, will the %z be ok ?
    return df.to_csv(path_or_buf=None, sep=',', decimal='.', na_rep='', encoding=charset,
                     index=False, date_format='%Y-%m-%dT%H:%M:%S.000%z')


def dfs_to_csvs(dfs,          # type: Dict[str, pandas.DataFrame]
                charset=None  # type: str
                ):
    # type: (...) -> Dict[str, str]
    """
    Converts each of the DataFrames in the provided dictionary to a csv, typically to store it on blob storage for
    Batch AzureML calls. All CSV are returned in a dictionary with the same keys.

    WARNING: datetime columns are converted in ISO format but the milliseconds are ignored and set to zero.
    See `df_to_csv` for details

    :param dfs: a dictionary containing input names and input content (each input content is a DataFrame)
    :param charset: the charset to use for csv encoding
    :return: a dictionary containing the string representations of the Csv inputs to store on the blob storage
    """
    validate('dfs', dfs, instance_of=dict)

    return {input_name: df_to_csv(inputDf, df_name=input_name, charset=charset)
            for input_name, inputDf in dfs.items()}


def csv_to_df(csv_buffer_or_str_or_filepath,  # type: Union[str, StringIO, BytesIO]
              csv_name=None                   # type: str
              ):
    # type: (...) -> pandas.DataFrame
    """
    Converts the provided csv to a DatFrame, typically to read it from blob storage for Batch AzureML calls.
    Helper method to ensure consistent reading in particular for timezones and datetime parsing

    :param csv_buffer_or_str_or_filepath:
    :param csv_name: the name of the DataFrame, for error messages
    :return:
    """
    validate(csv_name, csv_buffer_or_str_or_filepath)

    # pandas does not accept string. create a buffer
    if isinstance(csv_buffer_or_str_or_filepath, str):
        csv_buffer_or_str_or_filepath = create_reading_buffer(csv_buffer_or_str_or_filepath)

    # read without parsing dates
    res = pandas.read_csv(csv_buffer_or_str_or_filepath, sep=',', decimal='.')  # infer_dt_format=True, parse_dates=[0]

    # -- try to infer datetime columns
    convert_all_datetime_columns(res)

    # -- additionally we automatically configure the timezone as UTC
    localize_all_datetime_columns(res)

    return res


def csvs_to_dfs(csv_dict  # type: Dict[str, str]
                ):
    # type: (...) -> Dict[str, pandas.DataFrame]
    """
    Helper method to read CSVs compliant with AzureML web service BATCH inputs/outputs, into a dictionary of DataFrames

    :param csv_dict:
    :return:
    """
    validate('csv_dict', csv_dict, instance_of=dict)

    return {input_name: csv_to_df(inputCsv, csv_name=input_name)
            for input_name, inputCsv in csv_dict.items()}


def df_to_azmltable(df,                         # type: pandas.DataFrame
                    table_name=None,            # type: str
                    swagger=False,              # type: bool
                    mimic_azml_output=False,    # type: bool
                    ):
    # type: (...) -> Union[AzmlTable, AzmlOutputTable]
    """
    Converts the provided DataFrame to a dictionary or list in the same format than the JSON expected by AzureML in
    the Request-Response services. Note that contents are kept as is (values are not converted to string yet)

    :param df: the DataFrame to convert
    :param table_name: the table name for error messages
    :param swagger: a boolean indicating if the swagger format should be used (more verbose). Default: False
    :param mimic_azml_output: set this to True if the result should be wrapped in a dictionary like AzureML outputs.
        This is typically needed if you wish to mimic an AzureML web service's behaviour, for a mock web server.
    :return:
    """
    validate(table_name, df, instance_of=pandas.DataFrame)

    if mimic_azml_output:
        # use this method recursively, in 'not output' mode
        return {'type': 'table', 'value': df_to_azmltable(df, table_name=table_name, swagger=swagger)}
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
            # --> dont do type conversion, AzureML type mapping does not seem to be reliable enough.
            return {'ColumnNames': col_names,
                    "Values": df.values.tolist()}


def dfs_to_azmltables(dfs  # type: Dict[str, pandas.DataFrame]
                      ):
    # type: (...) -> Dict[str, Dict[str, Union[str, Dict[str, List]]]]
    """
    Converts a dictionary of DataFrames into a dictionary of dictionaries following the structure
    required for AzureML JSON conversion

    :param dfs: a dictionary containing input names and input content (each input content is a DataFrame)
    :return: a dictionary of tables represented as dictionaries
    """
    validate('dfs', dfs, instance_of=dict)

    # resultsDict = {}
    # for dfName, df in DataFramesDict.items():
    #     resultsDict[dfName] = Df_to_AzmlTable(df, dfName)
    # return resultsDict

    return {df_name: df_to_azmltable(df, table_name=df_name) for df_name, df in dfs.items()}


def azmltable_to_df(azmltable,             # type: Union[AzmlTable, AzmlOutputTable]
                    is_azml_output=False,  # type: bool
                    table_name=None        # type: str
                    ):
    # type: (...) -> pandas.DataFrame
    """
    Converts a parsed AzureML table (JSON-like dictionary or list obtained from parsing the json body) into a
    DataFrame. Since two formats exist (one for inputs and one for outputs), there is a parameter you can use to
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
                return azmltable_to_df(azmltable['value'], table_name=table_name)
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
                col_names = list(azmltable[0].keys())
                for i, row in enumerate(azmltable):
                    try:
                        row_vals = [row[k] for k in col_names]
                        values.append(row_vals)
                        if len(row) > len(col_names):
                            new_cols = set(row.keys()) - set(col_names)
                            raise ValueError("Columns are present in row #%s but not in the first row: "
                                             "%s" % (i + 1, new_cols))
                    except KeyError as e:
                        raise ValueError("A column is missing in row #%s: %s" % (i + 1, e))
            else:
                col_names = []

        elif 'ColumnNames' in azmltable.keys() and 'Values' in azmltable.keys():
            # non-swagger format
            values = azmltable['Values']
            col_names = azmltable['ColumnNames']
        else:
            raise ValueError("object should be a list or a dictionary with two fields ColumnNames and Values, "
                             "found: %s for table object: %s" % (azmltable.keys(), table_name))

        if len(values) > 0:
            # # create DataFrame manually
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
            writer.writerows([col_names])
            writer.writerows(values)
            # -- and then we parse with pandas
            res = csv_to_df(create_reading_buffer(buffer.getvalue()))  # StringIO
            buffer.close()

        else:
            # empty DataFrame
            res = pandas.DataFrame(columns=col_names)

        return res


def azmltables_to_dfs(azmltables_dict,  # type: Dict[str, Dict[str, Union[str, Dict[str, List]]]]
                      is_azureml_output=False  # type: bool
                      ):
    # type: (...) -> Dict[str, pandas.DataFrame]

    validate('azmltables_dict', azmltables_dict, instance_of=dict)

    return {input_name: azmltable_to_df(dict_table, is_azml_output=is_azureml_output, table_name=input_name)
            for input_name, dict_table in azmltables_dict.items()}


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


def params_dict_to_params_df(params_dict  # type: Dict[str, Any]
                             ):
    # type: (...) -> pandas.DataFrame
    """
    Converts a parameter dictionary into a parameter DataFrame

    :param params_dict:
    :return:
    """
    validate('params_dict', params_dict, instance_of=dict)

    # create a single-row DataFrame
    return pandas.DataFrame(params_dict, index=[0])


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
    return json.dumps(azmltable, default=azml_json_serializer)

    
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


def convert_all_datetime_columns(df):
    """
    Utility method to try to convert all datetime columns in the provided DataFrame, inplace.
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
