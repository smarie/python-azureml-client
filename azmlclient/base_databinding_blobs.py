from io import BytesIO   # to handle byte strings
from io import StringIO  # to handle unicode strings

from requests import Session
from valid8 import validate
import pandas as pd

try:  # python 3.5+
    from typing import Dict, Union, List, Any, Tuple

    # a few predefined type hints
    SwaggerModeAzmlTable = List[Dict[str, Any]]
    NonSwaggerModeAzmlTable = Dict[str, Union[List[str], List[List[Any]]]]
    AzmlTable = Union[SwaggerModeAzmlTable, NonSwaggerModeAzmlTable]
    AzmlOutputTable = Dict[str, Union[str, AzmlTable]]
    AzmlBlobTable = Dict[str, str]
except ImportError:
    pass


from azure.storage.blob import BlockBlobService, ContentSettings
from azmlclient.base_databinding import csv_to_df, df_to_csv


def csv_to_blob_ref(csv_str,  # type: str
                    blob_service,  # type: BlockBlobService
                    blob_container,  # type: str
                    blob_name,  # type: str
                    blob_path_prefix=None,  # type: str
                    charset=None  # type: str
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
              "DataFrames. We recommend setting charset to None or utf-8 ")

    # validate inputs (the only one that is not validated below)
    validate('csv_str', csv_str, instance_of=str)

    # 1- first create the references in order to check all params are ok
    blob_reference, blob_full_name = create_blob_ref(blob_service=blob_service, blob_container=blob_container,
                                                     blob_path_prefix=blob_path_prefix, blob_name=blob_name)

    # -- push blob
    blob_stream = BytesIO(csv_str.encode(encoding=charset))
    # noinspection PyTypeChecker
    blob_service.create_blob_from_stream(blob_container, blob_full_name, blob_stream,
                                         content_settings=ContentSettings(content_type='text.csv',
                                                                          content_encoding=charset))
    # (For old method with temporary files: see git history)

    return blob_reference


def csvs_to_blob_refs(csvs_dict,  # type: Dict[str, str]
                      blob_service,  # type: BlockBlobService
                      blob_container,  # type: str
                      blob_path_prefix=None,  # type: str
                      blob_name_prefix=None,  # type: str
                      charset=None  # type: str
                      ):
    # type: (...) -> Dict[str, Dict[str, str]]
    """
    Utility method to push all inputs described in the provided dictionary into the selected blob storage on the cloud.
    Each input is an entry of the dictionary and containing the description of the input reference as dictionary.
    The string will be written to the blob using the provided charset.
    Note: files created on the blob storage will have names generated from the current time and the input name, and will
     be stored in

    :param csvs_dict:
    :param blob_service:
    :param blob_container:
    :param blob_path_prefix: the optional prefix that will be prepended to all created blobs in the container
    :param blob_name_prefix: the optional prefix that will be prepended to all created blob names in the container
    :param charset: an optional charset to be used, by default utf-8 is used
    :return: a dictionary of "by reference" input descriptions as dictionaries
    """

    validate('csvs_dict', csvs_dict, instance_of=dict)
    if blob_name_prefix is None:
        blob_name_prefix = ""
    else:
        validate('blob_name_prefix', blob_name_prefix, instance_of=str)

    return {blobName: csv_to_blob_ref(csvStr, blob_service=blob_service, blob_container=blob_container,
                                      blob_path_prefix=blob_path_prefix, blob_name=blob_name_prefix + blobName, 
                                      charset=charset)
            for blobName, csvStr in csvs_dict.items()}


def blob_ref_to_csv(blob_reference,  # type: AzmlBlobTable
                    blob_name=None,  # type: str
                    encoding=None,  # type: str
                    requests_session=None  # type: Session
                    ):
    """
    Reads a CSV stored in a Blob Storage and referenced according to the format defined by AzureML, and transforms
    it into a DataFrame.

    :param blob_reference: a (AzureML json-like) dictionary representing a table stored as a csv in a blob storage.
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
        raise ValueError(
            'Blob reference is invalid: it should contain ConnectionString and RelativeLocation fields')


def blob_refs_to_csvs(blob_refs,  # type: Dict[str, Dict[str, str]]
                      charset=None,  # type: str
                      requests_session=None  # type: Session
                      ):
    # type: (...) -> Dict[str, str]
    """

    :param blob_refs:
    :param charset:
    :param requests_session: an optional Session object that should be used for the HTTP communication
    :return:
    """

    validate('blob_refs', blob_refs, instance_of=dict)

    return {blobName: blob_ref_to_csv(csvBlobRef, encoding=charset, blob_name=blobName, 
                                      requests_session=requests_session)
            for blobName, csvBlobRef in blob_refs.items()}


def df_to_blob_ref(df,  # type: pd.DataFrame
                   blob_service,  # type: BlockBlobService
                   blob_container,  # type: str
                   blob_name,  # type: str
                   blob_path_prefix=None,  # type: str
                   charset=None  # type: str
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
    csv_str = df_to_csv(df, df_name=blob_name, charset=charset)

    # upload it
    return csv_to_blob_ref(csv_str, blob_service=blob_service, blob_container=blob_container,
                           blob_path_prefix=blob_path_prefix, blob_name=blob_name, charset=charset)


def dfs_to_blob_refs(dfs_dict,  # type: Dict[str, pd.DataFrame]
                     blob_service,  # type: BlockBlobService
                     blob_container,  # type: str
                     blob_path_prefix=None,  # type: str
                     blob_name_prefix=None,  # type: str
                     charset=None  # type: str
                     ):
    # type: (...) -> Dict[str, Dict[str, str]]

    validate('DataFramesDict', dfs_dict, instance_of=dict)

    return {blobName: df_to_blob_ref(csvStr, blob_service=blob_service, blob_container=blob_container,
                                     blob_path_prefix=blob_path_prefix, blob_name=blob_name_prefix + blobName, 
                                     charset=charset)
            for blobName, csvStr in dfs_dict.items()}


def blob_ref_to_df(blob_reference,  # type: AzmlBlobTable
                   blob_name=None,  # type: str
                   encoding=None,  # type: str
                   requests_session=None  # type: Session
                   ):
    """
    Reads a CSV blob referenced according to the format defined by AzureML, and transforms it into a DataFrame

    :param blob_reference: a (AzureML json-like) dictionary representing a table stored as a csv in a blob storage.
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

    blob_content = blob_ref_to_csv(blob_reference, blob_name=blob_name, encoding=encoding, 
                                   requests_session=requests_session)

    if len(blob_content) > 0:
        # convert to DataFrame
        return csv_to_df(StringIO(blob_content), blob_name)
    else:
        # empty blob > empty DataFrame
        return pd.DataFrame()


def blob_refs_to_dfs(blob_refs,  # type: Dict[str, Dict[str, str]]
                     charset=None,  # type: str
                     requests_session=None  # type: Session
                     ):
    # type: (...) -> Dict[str, pd.DataFrame]
    """
    Reads Blob references, for example responses from an AzureMl Batch web service call, into a dictionary of
    pandas DataFrame

    :param blob_refs: the json output description by reference for each output
    :param charset:
    :param requests_session: an optional Session object that should be used for the HTTP communication
    :return: the dictionary of corresponding DataFrames mapped to the output names
    """
    validate('blob_refs', blob_refs, instance_of=dict)

    return {blobName: blob_ref_to_df(csvBlobRef, encoding=charset, blob_name=blobName, 
                                     requests_session=requests_session)
            for blobName, csvBlobRef in blob_refs.items()}


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
    connection_str = _get_blob_service_connection_string(blob_service)

    # check the blob path prefix, append a trailing slash if necessary
    blob_path_prefix = _get_valid_blob_path_prefix(blob_path_prefix)

    # output reference and full name
    blob_full_name = '%s%s.csv' % (blob_path_prefix, blob_name)
    relative_location = "%s/%s" % (blob_container, blob_full_name)
    output_ref = {'ConnectionString': connection_str,
                  'RelativeLocation': relative_location}

    return output_ref, blob_full_name


def create_blob_refs(blob_service,  # type: BlockBlobService
                     blob_container,  # type: str
                     blob_names,  # type: List[str]
                     blob_path_prefix=None,  # type: str
                     blob_name_prefix=None  # type: str
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
    return {blob_name: create_blob_ref(blob_service, blob_container, blob_name_prefix + blob_name,
                                       blob_path_prefix=blob_path_prefix)[0]
            for blob_name in blob_names}


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


def _get_blob_service_connection_string(blob_service  # type: BlockBlobService
                                        ):
    # type: (...) -> str
    """
    Utility method to get the connection string for a blob storage service (currently the BlockBlobService does
    not provide any method to do that)

    :param blob_service:
    :return:
    """
    validate('blob_service', blob_service, instance_of=BlockBlobService)

    return "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s" \
           "" % (blob_service.account_name, blob_service.account_key)
