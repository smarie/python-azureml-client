import json
import time

from datetime import datetime
from warnings import warn

from six import raise_from

try:
    from urllib.error import HTTPError as Urllib_HTTPError
except ImportError:
    # create a dummy class
    class Urllib_HTTPError(Exception):
        pass

import pandas as pd
import requests

try:  # python 3.5+
    from typing import List, Dict, Tuple, Any, Union, Optional
except ImportError:
    pass

from .requests_utils import set_http_proxy
from .base_databinding import AzmlException, dfs_to_azmltables, params_df_to_params_dict, azmltable_to_json, \
    json_to_azmltable, azmltables_to_dfs


class IllegalJobStateException(Exception):
    """ This is raised whenever a job has illegal state"""


class JobExecutionException(Exception):
    """ This is raised whenever a job ended in failed mode"""


def create_session_for_proxy(http_proxyhost,                  # type: str
                             http_proxyport,                  # type: int
                             https_proxyhost=None,            # type: str
                             https_proxyport=None,            # type: int
                             use_http_for_https_proxy=False,  # type: bool
                             ssl_verify=None                  # type: Union[bool, str]
                             ):
    # type: (...) -> requests.Session
    """
    DEPRECATED - users should rather create a Session() and use set_http_proxy(session, **kwargs) instead

    Helper method to configure the request package to use the proxy of your choice and adapt the SSL certificate
    validation accordingly.

    ```python
    # create a temporary Session to use Fiddler as the network proxy
    debug_session = create_session_for_proxy('localhost', 8888, use_http_for_https_proxy=True, ssl_verify=False)

    # use that session in a, AzureML web service call
    execute_rr(..., requests_session=debug_session)
    ```

    :param http_proxyhost: mandatory proxy host for http
    :param http_proxyport: mandatory proxy port for http
    :param https_proxyhost: optional proxy host for https. If none is provided, http_proxyhost will be used
    :param https_proxyport: optional proxy port for https. If none is provided, http_proxyport will be used
    :param use_http_for_https_proxy: optional, if set to true the http protocol will be used to initiate communications
        with the proxy even for https calls (then calls will be done in https as usual).
    :param ssl_verify: optional ssl verification parameter. It may either be the path to an additional certificate
        to trust (recommended), or a boolean to enable (default)/disable (not recommended ! use only in debug mode !)
        certificate validation.
        See here for details : http://docs.python-requests.org/en/master/user/advanced/#ssl-cert-verification
    :return: a requests.Session object that you may use with the rest of the library
    """
    warn("This method is deprecated - please create a Session() and use set_http_proxy(session, **kwargs) instead")

    session = requests.Session()

    set_http_proxy(session,
                   http_proxyhost=http_proxyhost, http_proxyport=http_proxyport,
                   https_proxyhost=https_proxyhost, https_proxyport=https_proxyport,
                   use_http_proxy_for_https_requests=use_http_for_https_proxy)

    if ssl_verify is not None:
        session.verify = ssl_verify

    return session


def execute_rr(api_key,                                # type: str
               base_url,                               # type: str
               inputs=None,                            # type: Dict[str, pd.DataFrame]
               params=None,                            # type: Union[pd.DataFrame, Dict[str, Any]]
               output_names=None,                      # type: List[str]
               only_keep_selected_output_names=False,  # type: bool
               use_swagger_format=False,               # type: bool
               replace_NaN_with=None,                  # type: Any
               replace_NaT_with=None,                  # type: Any
               requests_session=None                   # type: requests.Session
               ):
    # type: (...) -> Dict[str, pd.DataFrame]
    """
    Executes an AzureMl web service in request-response (RR) mode. This mode is typically used when the web service does
    not take too long to execute. For longer operations you should use the batch mode (BES).

    :param api_key: the api key for the AzureML web service to call. For example 'fdjmxkqktcuhifljflkdmw'
    :param base_url: the URL of the AzureML web service to call. It should not contain the "execute". This is typically
        in the form 'https://<geo>.services.azureml.net/workspaces/<wId>/services/<sId>'.
    :param inputs: an optional dictionary containing the inputs, by name. Inputs should be DataFrames.
    :param params: an optional dictionary containing the parameters by name, or a DataFrame containing the parameters.
    :param output_names: an optional list of expected output names, for automatic validation.
    :param only_keep_selected_output_names: a boolean (default False) to indicate if only the outputs selected in
        `output_names` should be kept in the returned dictionary.
    :param use_swagger_format: a boolean (default False) indicating if the 'swagger' azureml format should be used
            to format the data tables in json payloads.
    :param requests_session: an optional requests.Session object, for example created from create_session_for_proxy()
    :return: a dictionary of outputs, by name. Outputs are DataFrames
    """
    # quick check before spending time with the query
    if output_names is None and only_keep_selected_output_names:
        raise ValueError("`only_keep_selected_output_names` can only be used with a non-None list of "
                         "`output_names`")

    # 0- Create the generic request-response client
    rr_client = RequestResponseClient(requests_session=requests_session, use_swagger_format=use_swagger_format,
                                      replace_NaN_with=replace_NaN_with, replace_NaT_with=replace_NaT_with)

    # 1- Create the query body
    request_body = rr_client.create_request_body(inputs, params)

    # 2- Execute the query and receive the response body
    response_body = rr_client.execute_rr(base_url, api_key, request_body)

    # 3- parse the response body into a dictionary of DataFrames
    result_dfs = rr_client.read_response_json_body(response_body, output_names)

    # 4- possibly filter outputs
    if only_keep_selected_output_names:
        selected_dfs = {k: result_dfs[k] for k in output_names}
        return selected_dfs
    else:
        return result_dfs


def execute_bes(api_key,                              # type: str
                base_url,                             # type: str
                blob_storage_account,                 # type: str
                blob_storage_apikey,                  # type: str
                blob_container,                       # type: str
                blob_path_prefix=None,                # type: str
                blob_charset=None,                    # type: str
                inputs=None,                          # type: Dict[str, pd.DataFrame]
                params=None,
                output_names=None,                    # type: List[str]
                nb_seconds_between_status_queries=5,  # type: int
                requests_session=None                 # type: requests.Session
                ):
    """
    Executes an AzureMl web service in batch mode (BES: Batch Execution Service).

    Its inputs are the same than `execute_rr` but in addition it takes information about the blob storage service to
    use. Indeed in batch mode, all inputs and outputs go through an intermediate blob storage.

    The AzureML job status is queried every 5 seconds by default, you may wish to change that number with
    `nb_seconds_between_status_queries`.

    :param api_key: the api key for the service to call
    :param base_url: the URL of the service to call
    :param blob_storage_account: the storage account to use to store the inputs and outputs
    :param blob_storage_apikey: the storage api key to use to store the inputs and outputs
    :param blob_container: the container in the blob storage, that will be used to store the inputs and outputs
    :param blob_path_prefix: an optional prefix that will be used to store the blobs
    :param blob_charset: optional encoding of files used on the blob storage
    :param inputs: an optional dictionary containing the inputs, by name. Inputs should be DataFrames.
    :param params: an optional dictionary containing the parameters by name, or a DataFrame containing the parameters.
    :param output_names: an optional list of expected output names. Note that contrary to rr mode, no outputs will be
        provided if this is empty.
    :param nb_seconds_between_status_queries: nb of seconds that the engine waits between job status queries. By
        default this is set to 5.
    :param requests_session: an optional requests.Session object, for example created from create_session_for_proxy()
    :return: a dictionary of outputs, by name. Outputs are DataFrames
    """

    # 0 create the blob service client and the generic batch mode client
    batch_client = BatchClient(requests_session=requests_session)

    # if we're here without error that means that `azure-storage` is available
    from azure.storage.blob import BlockBlobService
    from azmlclient.base_databinding_blobs import blob_refs_to_dfs

    blob_service = BlockBlobService(account_name=blob_storage_account, account_key=blob_storage_apikey,
                                    request_session=requests_session)

    # 1- Push inputs to blob storage and create output references
    print('Pushing inputs to blob storage')
    input_refs, output_refs = batch_client.push_inputs_to_blob__and__create_output_references(inputs,
                                                                                              output_names=output_names,
                                                                                              blob_service=blob_service,
                                                                                              blob_container=blob_container,
                                                                                              blob_path_prefix=blob_path_prefix,
                                                                                              charset=blob_charset)

    # 2- Create the query body
    request_body = batch_client.create_request_body(input_refs, params, output_refs)

    # 3- Perform the call
    json_job_id = None
    try:
        # -- a) create the job
        print('Creating job')
        json_job_id = batch_client.execute_batch_createJob(base_url, api_key, request_body)

        # -- b) start the job
        print('Starting job ' + str(json_job_id))
        batch_client.execute_batch_startJob(base_url, api_key, json_job_id)
        print('Job ' + str(json_job_id) + ' started')

        # -- polling loop
        outputs_refs2 = None
        while outputs_refs2 is None:
            # -- c) poll job status
            print('Polling job status for job ' + str(json_job_id))
            statusOrResult = batch_client.execute_batch_getJobStatusOrResult(base_url, api_key, json_job_id)

            # -- e) check the job status and read response into a dictionary
            outputs_refs2 = batch_client.read_status_or_result(statusOrResult)

            # wait
            print('Waiting ' + str(nb_seconds_between_status_queries) + 's until next call.')
            time.sleep(nb_seconds_between_status_queries)

    finally:
        # -- e) delete the job
        if not (json_job_id is None):
            print('Deleting job ' + str(json_job_id))
            batch_client.execute_batch_deleteJob(base_url, api_key, json_job_id)

    # 4- Retrieve the outputs
    print('Job ' + str(json_job_id) + ' completed, results: ')
    print(json.dumps(outputs_refs2, indent=4))

    print('Retrieving the outputs from the blob storage')

    # dont use the output of the job status (outputs_refs2), it does not contain the connectionString
    result_dfs = blob_refs_to_dfs(output_refs, requests_session=requests_session)

    return result_dfs


class BaseHttpClient(object):
    """
    Base class for our http clients. It contains a `requests.Session` object and
    """
    def __init__(self,
                 requests_session=None,    # type: requests.Session
                 ):
        """
        Constructor with an optional `requests.Session` to use for subsequent calls.
        Also you can declare to use the 'swagger' AzureML format for data table formatting (not enabled by default
        because it is more verbose).

        :param requests_session:
        """
        # optionally create a session
        if requests_session is None:
            requests_session = requests.Session()

        # store it
        self.session = requests_session

        # if one day we want to reuse Microsoft's Http client to align with blockblobservice, they have this:
        # self._httpclient = _HTTPClient(
        #     protocol=DEFAULT_PROTOCOL,
        #     session=request_session,
        #     timeout=SOCKET_TIMEOUT,
        # )

    def azureml_http_call(self,
                          url,             # type: str
                          api_key,         # type: str
                          method,          # type: str
                          body_str=None,   # type: Optional[str]
                          charset='utf-8'  # type: str
                          ):
        # type: (...) -> str
        """
        Performs an HTTP(s) request to an AzureML web service, whatever it is.

        This method

         - sets the Authorization header wth the api key
         - optionally encodes the input body according to the charset selected
         - performs

        :param api_key: the api key for this AzureML call.
        :param body_str: the input body, for PUT and POST methods
        :param url: the url to call
        :param method: the HTTP verb to use ('GET', 'PUT', 'POST'...)
        :param charset: the optional charset to use to encode the body. Default is 'utf-8'
        :return: the response body
        """
        # fill the information about the query to perform
        headers = {'Authorization': ('Bearer ' + api_key)}

        # encode the string as bytes using the charset
        if body_str is not None:
            json_body_encoded_with_charset = str.encode(body_str, encoding=charset)
            headers['Content-Type'] = 'application/json; charset=' + charset
        else:
            json_body_encoded_with_charset = None

        # finally execute
        json_result = self.http_call(json_body_encoded_with_charset, headers, method, url)

        return json_result

    def http_call(self,
                  body,
                  headers,
                  method,  # type: str
                  url
                  ):
        """
        Sub-routine for HTTP web service call. If Body is None, a GET is performed

        :param body:
        :param headers:
        :param method
        :param url:
        :return:
        """
        try:
            # Send the request
            response = self.session.request(method, url, headers=headers, data=body or None)

            # Parse the response
            http_status = int(response.status_code)

            # Possibly raise associated exceptions
            response.raise_for_status()

            # Decode contents
            # headers not useful anymore : encoding is automatically used to read the body when calling response.text
            # respheaders = {key.lower(): name for key, name in response.headers.items()}
            jsonResult = response.text
            return jsonResult

        except requests.exceptions.HTTPError as error:
            print("The request failed with status code: %s" % error.response.status_code)
            # Print the headers - they include the request ID and timestamp, which are useful for debugging the failure
            print(error.response.headers)
            raise AzmlException(error)

        except Urllib_HTTPError as error:
            print("The request failed with status code: %s" + error.code)
            # Print the headers - they include the request ID and timestamp, which are useful for debugging the failure
            print(error.info())
            raise AzmlException(error)


class RequestResponseClient(BaseHttpClient):
    """
    A class providing static methods to perform Request-response calls to AzureML web services
    """

    def __init__(self,
                 requests_session=None,     # type: requests.Session
                 use_swagger_format=False,  # type: bool
                 replace_NaN_with=None,     # type: Any
                 replace_NaT_with=None,     # type: Any
                 ):
        """
        Constructor with an optional `requests.Session` to use for subsequent calls.
        Also you can declare to use the 'swagger' AzureML format for data table formatting (not enabled by default
        because it is more verbose).

        :param requests_session:
        :param use_swagger_format: a boolean (default False) indicating if the 'swagger' azureml format should be used
            to format the data tables in json payloads.
        """
        # save swagger format
        self.use_swagger_format = use_swagger_format
        self.replace_NaN_with = replace_NaN_with
        self.replace_NaT_with = replace_NaT_with

        # super constructor
        super(RequestResponseClient, self).__init__(requests_session=requests_session)

    def create_request_body(self,
                            input_df_dict=None,      # type: Dict[str, pd.DataFrame]
                            params_df_or_dict=None,  # type: Union[pd.DataFrame, Dict[str, Any]]
                            ):
        # type (...) -> str
        """
        Helper method to create a JSON AzureML web service input from inputs and parameters DataFrames

        :param input_df_dict: a dictionary containing input names and input content (each input content is a DataFrame)
        :param params_df_or_dict: a dictionary of parameter names and values
        :return: a string representation of the request JSON body (not yet encoded in bytes)
        """
        # handle optional arguments
        if input_df_dict is None:
            input_df_dict = {}
        if params_df_or_dict is None:
            params_df_or_dict = {}

        # inputs
        inputs = dfs_to_azmltables(input_df_dict, swagger_format=self.use_swagger_format,
                                   replace_NaN_with=self.replace_NaN_with, replace_NaT_with=self.replace_NaT_with)

        # params
        if isinstance(params_df_or_dict, dict):
            params = params_df_or_dict
        elif isinstance(params_df_or_dict, pd.DataFrame):
            params = params_df_to_params_dict(params_df_or_dict)
        else:
            raise TypeError('paramsDfOrDict should be a DataFrame or a dictionary, or None, found: '
                            + str(type(params_df_or_dict)))

        # final body : combine them into a single dictionary ...
        body_dict = {'Inputs': inputs, 'GlobalParameters': params}

        # ... and serialize as Json
        json_body_str = azmltable_to_json(body_dict)
        return json_body_str

    def create_response_body(self,
                             output_df_dict=None,  # type: Dict[str, pd.DataFrame]
                             ):
        """
        Creates a fake server response mimicking AzureML behaviour, from a dictionary of output dataframes.

        :param output_df_dict: a dictionary of {output_name: dataframe}
        :return:
        """
        res_dict = dfs_to_azmltables(output_df_dict, swagger_format=self.use_swagger_format, mimic_azml_output=True)
        return {"Results": res_dict}

    def execute_rr(self,
                   base_url,              # type: str
                   api_key,               # type: str
                   request_body_json,     # type: str
                   ):
        # type: (...) -> str
        """
        Performs a web service call to AzureML using Request-response mode (synchronous, by value).
        Supports Fiddler capture for debug.

        :param base_url:
        :param api_key:
        :param request_body_json: the json body of the web service request, as a string.
        :return: the json body of the response, as a string
        """
        rr_url = base_url + '/execute?api-version=2.0&details=true'
        if self.use_swagger_format:
            rr_url += '&format=swagger'

        json_result = self.azureml_http_call(url=rr_url, api_key=api_key, method='POST', body_str=request_body_json)

        return json_result

    @staticmethod
    def read_response_json_body(body_json,                             # type: str
                                output_names=None,                     # type: List[str]
                                ):
        # type: (...) -> Dict[str, pd.DataFrame]
        """
        Reads a response body from a request-response web service call, into a dictionary of pandas DataFrame

        :param body_json: the response body, already decoded as a string
        :param output_names: if a non-None list of output names is provided, each of these names must be present in
            the outputs dictionary, otherwise an error is raised.
        :return: the dictionary of corresponding DataFrames mapped to the output names
        """
        # first read the json as a dictionary
        result_dict = json_to_azmltable(body_json)

        # then transform it into a DataFrame
        result_dfs = azmltables_to_dfs(result_dict['Results'], is_azureml_output=True)

        if output_names is not None:
            # check the names
            missing = list(set(output_names) - set(result_dfs.keys()))
            if len(missing) > 0:
                raise Exception("Error : the following outputs are missing in the results: %s. Found outputs: %s"
                                "" % (missing, set(result_dfs.keys())))

        # return all outputs
        return result_dfs

    @staticmethod
    def decode_request_json_body(body_json  # type: str
                                 ):
        # type: (...) -> Tuple[Dict[str, pd.DataFrame], Dict]
        """
        Reads a request body from a request-response web service call, into a dictionary of pandas DataFrame + a
        dictionary of parameters. This is typically useful if you want to debug a request provided by someone else.

        :param body_json:
        :return:
        """
        # first read the json as a dictionary
        result_dct = json_to_azmltable(body_json)

        return azmltables_to_dfs(result_dct['Inputs']), result_dct['GlobalParameters']


class BatchClient(BaseHttpClient):
    """ This class provides static methods to call AzureML services in batch mode"""

    def __init__(self,
                 requests_session=None  # type: requests.Session
                 ):
        # check that the `azure-storage` package is installed
        try:
            from azure.storage.blob import BlockBlobService
        except ImportError as e:
            raise_from(ValueError("Please install `azure-storage==0.33` to use BATCH mode"), e)

        super(BatchClient, self).__init__(requests_session=requests_session)

    def push_inputs_to_blob__and__create_output_references(self,
                                                           inputs_df_dict,         # type: Dict[str, pd.DataFrame]
                                                           blob_service,           # type: BlockBlobService
                                                           blob_container,         # type: str
                                                           blob_path_prefix=None,  # type: str
                                                           charset=None,           # type: str
                                                           output_names=None       # type: List[str]
                                                           ):
        # type: (...) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]
        """
        Utility method to push all inputs from the provided dictionary into the selected blob storage on the cloud.
        Each input is an entry of the dictionary and should be a Dataframe.
        The inputs will be written to the blob using the provided charset.

        Files created on the blob storage will have a prefix generated from the current time, in order to
        quickly identify inputs pushed at the same time. For convenience, this prefix is provided as an output of this
        function so that outputs may be

        :param inputs_df_dict:
        :param blob_service:
        :param blob_container: the blob container name
        :param blob_path_prefix: the prefix to use for all blobs
        :param charset:
        :param output_names:
        :return: a tuple containing (1) a dictionary of "by reference" input descriptions
                and (2) a dictionary of "by reference" output descriptions
        """
        from azmlclient.base_databinding_blobs import dfs_to_blob_refs, create_blob_refs

        if output_names is None:
            output_names = []

        # 1- create unique blob naming prefix
        now = datetime.now()
        unique_blob_name_prefix = now.strftime("%Y-%m-%d_%H%M%S_%f")

        # 2- store INPUTS and retrieve references
        input_refs = dfs_to_blob_refs(inputs_df_dict, blob_service=blob_service,
                                                           blob_container=blob_container,
                                                           blob_path_prefix=blob_path_prefix,
                                                           blob_name_prefix=unique_blob_name_prefix + '-input-',
                                                           charset=charset)

        # 3- create OUTPUT references
        output_refs = create_blob_refs(blob_names=output_names, blob_service=blob_service,
                                                            blob_container=blob_container,
                                                            blob_path_prefix=blob_path_prefix,
                                                            blob_name_prefix=unique_blob_name_prefix + '-output-')

        return input_refs, output_refs

    @staticmethod
    def create_request_body(input_refs=None,         # type: Dict[str, Dict[str, str]]
                            params_df_or_dict=None,  # type: Union[Dict[str, Any], pd.DataFrame]
                            output_refs=None         # type: Dict[str, Dict[str, str]]
                            ):
        # type: (...) -> str
        """
        Helper method to create a JSON AzureML web service input in Batch mode, from 'by reference' inputs, and parameters as DataFrame

        :param input_refs: a dictionary containing input names and input references (each input reference is a dictionary)
        :param params_df_or_dict: a dictionary of parameter names and values
        :param output_refs: a dictionary containing output names and output references (each output reference is a dictionary)
        :return: a string representation of the request JSON body (not yet encoded in bytes)
        """

        # params
        if params_df_or_dict is None:
            params_df_or_dict = {}

        if isinstance(params_df_or_dict, dict):
            params = params_df_or_dict
        elif isinstance(params_df_or_dict, pd.DataFrame):
            params = params_df_to_params_dict(params_df_or_dict)
        else:
            raise TypeError(
                'paramsDfOrDict should be a DataFrame or a dictionary, or None, found: ' + str(type(params_df_or_dict)))

        # final body : combine them into a single dictionary ...
        body_dict = {'Inputs': input_refs, 'GlobalParameters': params, 'Outputs': output_refs}

        # ... and serialize as Json
        json_body_str = azmltable_to_json(body_dict)
        return json_body_str

    def execute_batch_createJob(self,
                                base_url,           # type: str
                                api_key,            # type: str
                                request_json_body,  # type: str
                                ):
        # type: (...) -> str
        """
        Performs a web service call to AzureML using Batch mode (asynchronous, by reference).
        Supports Fiddler capture for debug.

        :param base_url:
        :param api_key:
        :param request_json_body:
        :return:
        """

        batch_url = base_url + '/jobs?api-version=2.0'
        jsonJobId = self.azureml_http_call(url=batch_url, api_key=api_key, method='POST', body_str=request_json_body)

        # unquote the json Job Id
        if jsonJobId.startswith('"') and jsonJobId.endswith('"'):
            return jsonJobId[1:-1]
        else:
            return jsonJobId

    def execute_batch_startJob(self,
                               base_url,         # type: str
                               api_key,          # type: str
                               job_id,           # type: str
                               ):
        """
        Starts an AzureML Batch job (asynchronous, by reference).
        Supports Fiddler capture for debug.

        :param base_url:
        :param api_key:
        :param job_id:
        :return:
        """

        batch_url = base_url + '/jobs/' + job_id + '/start?api-version=2.0'

        self.azureml_http_call(url=batch_url, api_key=api_key, method='POST', body_str=None)
        return

    def execute_batch_getJobStatusOrResult(self,
                                           base_url,          # type: str
                                           api_key,           # type: str
                                           job_id,            # type: str
                                           ):
        # type: (...) -> str
        """
        Gets the status or the result of an AzureML Batch job (asynchronous, by reference).
        Supports Fiddler capture for debug.

        :param base_url:
        :param api_key:
        :param job_id:
        :return:
        """
        batch_url = base_url + '/jobs/' + job_id + '?api-version=2.0'
        json_job_status_or_result = self.azureml_http_call(url=batch_url, api_key=api_key, method='GET', body_str=None)
        return json_job_status_or_result

    def read_status_or_result(self,
                              jobstatus_or_result_json  # type: str
                              ):
        # type: (...) -> Dict[str, Dict[str, str]]
        """
        An alias to the static method

        :param jobstatus_or_result_json:
        :return:
        """
        return BatchClient.read_status_or_result_static(jobstatus_or_result_json)

    @staticmethod
    def read_status_or_result_static(jobstatus_or_result_json  # type: str
                                     ):
        # type: (...) -> Dict[str, Dict[str, str]]
        """
        Reads the status or the result of an AzureML Batch job (asynchronous, by reference).
        Throws an error if the status is an error, or an empty result if the status is a

        :param jobstatus_or_result_json:
        :return: the status as a dictionary, and throws an error if the job had an error
        """

        # first read the json as a dictionary
        result_dict = json_to_azmltable(jobstatus_or_result_json)

        try:
            status_code = result_dict['StatusCode']

            if status_code in ('3', 'Cancelled'):
                raise IllegalJobStateException("The job state is '%s' : cannot read the outcome" % status_code)

            elif status_code in ('2', 'Failed'):
                raise JobExecutionException("The job ended with an error : %s" % result_dict['Details'])

            elif status_code in ('0', 'NotStarted', '1', 'Running', '4', 'Finished'):
                jobstatus_or_result_json = result_dict['Results']

            else:
                raise IllegalJobStateException(
                    'The job state is ' + status_code + ' : unknown state')

            return jobstatus_or_result_json

        except KeyError:
            raise ValueError("Error reading job state : received %s" % result_dict)

    def execute_batch_deleteJob(self,
                                base_url,  # type: str
                                api_key,  # type: str
                                job_id,  # type: str
                                ):
        """
        Deletes an AzureML Batch job (asynchronous, by reference).
        Supports Fiddler capture for debug.

        :param base_url:
        :param api_key:
        :param job_id:
        :return:
        """
        batch_url = base_url + '/jobs/' + job_id + '?api-version=2.0'

        self.azureml_http_call(url=batch_url, api_key=api_key, method='DELETE', body_str=None)
        return


RR_Client = RequestResponseClient
"""Legacy alias"""

Batch_Client = BatchClient
"""Legacy alias"""
