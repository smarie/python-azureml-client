import json
import time
import urllib
from datetime import datetime

import pandas
import requests

try:  # python 3.5+
    from typing import List, Dict, Tuple
except ImportError:
    pass

from azure.storage.blob import BlockBlobService
from azmlclient.data_binding import AzmlException, Converters, Collection_Converters, _check_not_none_and_typed


class IllegalJobStateException(Exception):
    """ This is raised whenever a job has illegal state"""


class JobExecutionException(Exception):
    """ This is raised whenever a job ended in failed mode"""


def create_session_for_proxy(http_proxyhost,                  # type: str
                             http_proxyport,                  # type: int
                             https_proxyhost=None,            # type: str
                             https_proxyport=None,            # type: str
                             use_http_for_https_proxy=False,  # type: bool
                             ssl_verify=None
                             ):
    # type: (...) -> requests.Session
    """
    Helper method to configure the request package to use the proxy fo your choice and adapt the SSL certificate
    validation accordingly

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

    # ----- old with urllib
    # # fiddler is a localhost:8888 proxy
    # proxy = request.ProxyHandler({'http': '127.0.0.1:8888', 'https': '127.0.0.1:8888'})
    #
    # # don't verify SSL certificate, fiddler replaces the ones from outside with its own.
    # ctx = ssl.create_default_context()
    # ctx.check_hostname = False
    # ctx.verify_mode = ssl.CERT_NONE
    # https = request.HTTPSHandler(context=ctx)
    #
    # # chain the two options and install them
    # opener = request.build_opener(proxy, https)
    # request.install_opener(opener)

    _check_not_none_and_typed(http_proxyhost, str, 'http_proxyhost')
    _check_not_none_and_typed(http_proxyport, int, 'http_proxyport')

    https_proxyhost = https_proxyhost or http_proxyhost
    https_proxyport = https_proxyport or http_proxyport

    _check_not_none_and_typed(https_proxyhost, str, 'https_proxyhost')
    _check_not_none_and_typed(https_proxyport, int, 'https_proxyport')

    _check_not_none_and_typed(use_http_for_https_proxy, bool, 'use_http_for_https_proxy')

    https_proxy_protocol = 'http' if use_http_for_https_proxy else 'https'

    s = requests.Session()
    s.proxies = {
                    'http': 'http://' + http_proxyhost + ':' + str(http_proxyport),
                    'https': https_proxy_protocol + '://' + https_proxyhost + ':' + str(https_proxyport),
                }
    if not (ssl_verify is None):
        s.verify = ssl_verify

    # IMPORTANT : otherwise the environment variables will always have precedence over user settings
    s.trust_env = False

    return s


def execute_rr(api_key,               # type: str
               base_url,              # type: str
               inputs=None,           # type: typing.Dict[str, pandas.DataFrame]
               params=None,
               output_names=None,     # type: List[str]
               use_new_ws=False,      # type: bool
               requests_session=None  # type: requests.Session
               ):
    # type: (...) -> Dict[str, pandas.DataFrame]
    """
    Utility method to execute an AzureMl web service in request response mode

    :param api_key: the api key for the service to call
    :param base_url: the URL of the service to call
    :param inputs: an optional dictionary containing the inputs, by name. Inputs should be dataframes.
    :param params: an optional dictionary containing the parameters by name, or a dataframe containing the parameters.
    :param output_names: an optional list of expected output names
    :param use_new_ws: if True, calls will use the AzureML 'new Web services' format
    :param requests_session: an optional requests.Session object, for example created from create_session_for_proxy()
    :return: a dictionary of outputs, by name. Outputs are dataframes
    """

    # 0- Create the client
    rr_client = RR_Client(requests_session=requests_session)

    # 1- Create the query body
    request_body = rr_client.create_request_body(inputs, params)

    # 2- Execute the query and receive the response body
    response_body = rr_client.execute_rr(base_url, api_key, request_body,
                                         use_new_ws=use_new_ws)
    # 3- parse the response body into a dictionary of dataframes
    result_dfDict = rr_client.read_response_json_body(response_body, output_names)

    return result_dfDict


def execute_bes(api_key,                              # type: str
                base_url,                             # type: str
                blob_storage_account,                 # type: str
                blob_storage_apikey,                  # type: str
                blob_container,                       # type: str
                blob_path_prefix=None,                # type: str
                blob_charset=None,                    # type: str
                inputs=None,                          # type: Dict[str, pandas.DataFrame]
                params=None,
                output_names=None,                    # type: List[str]
                nb_seconds_between_status_queries=5,  # type: int
                use_new_ws=False,                     # type: bool
                requests_session=None                 # type: requests.Session
                ):
    """
    Utility method to execute an azureML web service in batch mode. Job status is queried every 5 seconds by default, you may wish to change that number.

    :param api_key: the api key for the service to call
    :param base_url: the URL of the service to call
    :param blob_storage_account: the storage account to use to store the inputs and outputs
    :param blob_storage_apikey: the storage api key to use to store the inputs and outputs
    :param blob_container: the container in the blob storage, that will be used to store the inputs and outputs
    :param blob_path_prefix: an optional prefix that will be used to store the blobs
    :param blob_charset: optional encoding of files used on the blob storage
    :param inputs: an optional dictionary containing the inputs, by name. Inputs should be dataframes.
    :param params: an optional dictionary containing the parameters by name, or a dataframe containing the parameters.
    :param output_names: an optional list of expected output names. Note that contrary to rr mode, no outputs will be provided if this is empty.
    :param nb_seconds_between_status_queries: nb of seconds that the engine waits between job status queries. By default this is set to 5.
    :param use_new_ws: if True, calls will use the AzureML 'new Web services' format
    :param requests_session: an optional requests.Session object, for example created from create_session_for_proxy()
    :return: a dictionary of outputs, by name. Outputs are dataframes
    """

    # 0 create blob service and batch clients
    blob_service = BlockBlobService(account_name=blob_storage_account, account_key=blob_storage_apikey, request_session=requests_session)
    batch_client = Batch_Client(requests_session=requests_session)

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
        json_job_id = batch_client.execute_batch_createJob(base_url, api_key, request_body, use_new_ws=use_new_ws)

        # -- b) start the job
        print('Starting job ' + str(json_job_id))
        batch_client.execute_batch_startJob(base_url, api_key, json_job_id, use_new_ws=use_new_ws)
        print('Job ' + str(json_job_id) + ' started')

        # -- polling loop
        outputs_refs2 = None
        while outputs_refs2 is None:
            # -- c) poll job status
            print('Polling job status for job ' + str(json_job_id))
            statusOrResult = batch_client.execute_batch_getJobStatusOrResult(base_url, api_key, json_job_id,
                                                                            use_new_ws=use_new_ws)

            # -- e) check the job status and read response into a dictionary
            outputs_refs2 = batch_client.read_status_or_result(statusOrResult)

            # wait
            print('Waiting ' + str(nb_seconds_between_status_queries) + 's until next call.')
            time.sleep(nb_seconds_between_status_queries)

    finally:
        # -- e) delete the job
        if not (json_job_id is None):
            print('Deleting job ' + str(json_job_id))
            batch_client.execute_batch_deleteJob(base_url, api_key, json_job_id,
                                                use_new_ws=use_new_ws)

    # 4- Retrieve the outputs
    print('Job ' + str(json_job_id) + ' completed, results: ')
    print(json.dumps(outputs_refs2, indent=4))

    print('Retrieving the outputs from the blob storage')

    # dont use the output of the status, it does not contain the connectionString
    # resultDataframes = Collection_Converters.BlobCsvRefDict_to_DfDict(outputs_refs2, requests_session=requests_session)
    resultDataframes = Collection_Converters.blobcsvrefdict_to_dfdict(output_refs, requests_session=requests_session)

    return resultDataframes


class _BaseHttpClient(object):

    def __init__(self, requests_session=None):

        # create and store a session
        self.session = requests_session or requests.Session()

        #if one day we want to reuse Microsoft's Http client to align with blockblobservice, they have this:
        # self._httpclient = _HTTPClient(
        #     protocol=DEFAULT_PROTOCOL,
        #     session=request_session,
        #     timeout=SOCKET_TIMEOUT,
        # )

    @staticmethod
    def _azureml_simple_httpCall(api_key,
                                 requestJsonBodyStr,
                                 url,
                                 method,
                                 use_new_ws=False  # type: bool
                                 ):
        # create a default client
        c = _BaseHttpClient()
        return c._azureml_httpCall(api_key, requestJsonBodyStr, url, method, use_new_ws)


    def _azureml_httpCall(self,
                          api_key,
                          requestJsonBodyStr,
                          url,
                          method,
                          use_new_ws=False  # type: bool
                          ):
        """
        Utility method to perform an HTTP request to AzureML service.

        :param api_key:
        :param requestJsonBodyStr:
        :param url:
        :param method:
        :param use_new_ws:
        :return:
        """
        # TODO support new web services mode
        if use_new_ws:
            raise Exception('The AzureML *new* web services are not supported')

        # then fill the information about the query to perform
        headers = {'Authorization': ('Bearer ' + api_key)}

        if not (requestJsonBodyStr is None):
            # first encode the string as bytes using the charset
            charset = 'utf-8'
            json_body_encoded_with_charset = str.encode(requestJsonBodyStr, encoding=charset)
            headers['Content-Type'] = 'application/json; charset=' + charset
        else:
            json_body_encoded_with_charset = None

        # finally execute
        jsonResult = self._http_call(json_body_encoded_with_charset, headers, method, url)

        return jsonResult

    def _http_call(self,
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
            # -------- Old with urllib
            # # normal mode
            # req = request.Request(url, data=body, headers=headers, method=method)
            # response = request.urlopen(req)
            #
            # # read the response
            # respbody = response.read()
            # respcharset = response.headers.get_content_charset()
            #
            # if respcharset is None:
            #     # this is typically a 'no content' body but just to be sure read it with utf-8
            #     jsonResult = str(object=respbody, encoding='utf-8')
            # else:
            #     jsonResult = str(object=respbody, encoding=response.headers.get_content_charset())


            # -------- New with requests
            # Send the request
            response = self.session.request(method,
                                            url,
                                            headers=headers,
                                            data=body or None)

            # Parse the response
            status = int(response.status_code)

            response.raise_for_status()

            # headers not useful anymore : encoding is automatically used to read the body when calling response.text
            # respheaders = {key.lower(): name for key, name in response.headers.items()}
            jsonResult = response.text

            return jsonResult

        except requests.exceptions.HTTPError as error:

            print("The request failed with status code: " + str(error.response.status_code))

            # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
            print(error.response.headers)

            raise AzmlException(error)

        except urllib.error.HTTPError as error:
            print("The request failed with status code: " + str(error.code))

            # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
            print(error.info())

            raise AzmlException(error)


class RR_Client(_BaseHttpClient):
    """
    A class providing static methods to perform Request-response calls to AzureML web services
    """

    def __init__(self,
                 requests_session=None  # type: requests.Session
                 ):
        super(RR_Client, self).__init__(requests_session=requests_session)

    def create_request_body(self,
                            input_df_dict=None,     # type: Dict[str, pandas.DataFrame]
                            params_df_or_dict=None  # type: pandas.DataFrame
                            ):
        # type -> str
        """
        An alias to the static method
        :param input_df_dict:
        :param params_df_or_dict:
        :return:
        """
        return RR_Client.create_request_body_static(input_df_dict, params_df_or_dict)

    @staticmethod
    def create_request_body_static(input_df_dict=None,     # type: Dict[str, pandas.DataFrame]
                                   params_df_or_dict=None  # type: pandas.DataFrame
                                   ):
        # type: (...) -> str
        """
        Helper method to create a JSON AzureML web service input from inputs and parameters dataframes

        :param input_df_dict: a dictionary containing input names and input content (each input content is a dataframe)
        :param params_df_or_dict: a dictionary of parameter names and values
        :return: a string representation of the request JSON body (not yet encoded in bytes)
        """

        # handle optional arguments
        if input_df_dict is None:
            input_df_dict = {}
        if params_df_or_dict is None:
            params_df_or_dict = {}

        # inputs
        inputs = Collection_Converters.dfdict_to_azmltablesdict(input_df_dict)

        # params
        if isinstance(params_df_or_dict, dict):
            params = params_df_or_dict
        elif isinstance(params_df_or_dict, pandas.DataFrame):
            params = Converters.paramdf_to_paramdict(params_df_or_dict)
        else:
            raise TypeError('paramsDfOrDict should be a dataframe or a dictionary, or None, found: '
                            + str(type(params_df_or_dict)))

        # final body : combine them into a single dictionary ...
        bodyDict = {'Inputs': inputs, 'GlobalParameters': params}

        # ... and serialize as Json
        jsonBodyStr = Converters.dict_to_jsonstr(bodyDict)
        return jsonBodyStr

    def execute_rr(self,
                   base_url,           # type: str
                   api_key,            # type: str
                   request_body_json,  # type: str
                   use_new_ws=False    # type: bool
                   ):
        # type: (...) -> str
        """
        Performs a web service call to AzureML using Request-response mode (synchronous, by value).
        Supports Fiddler capture for debug.

        :param base_url:
        :param api_key:
        :param request_body_json: the json body of the web service request, as a string.
        :param use_new_ws: a boolean to indicate to use the new AzureML web services
        :return: the json body of the response, as a string
        """

        requestResponseUrl = base_url + '/execute?api-version=2.0&details=true'

        jsonResult = self._azureml_httpCall(api_key, request_body_json, requestResponseUrl, 'POST', use_new_ws)
        return jsonResult

    def read_response_json_body(self,
                                body_json,         # type: str
                                output_names=None  # type: List[str]
                                ):
        # type: (...) -> Dict[str, pandas.DataFrame]
        """
        An alias to the static method
        :param body_json:
        :param output_names:
        :return:
        """
        return RR_Client.read_response_json_body_static(body_json, output_names)

    @staticmethod
    def read_response_json_body_static(body_json,         # type: str
                                       output_names=None  # type: List[str]
                                       ):
        # type: (...) -> Dict[str, pandas.DataFrame]
        """
        Reads a response body from a request-response web service call, into a dictionary of pandas dataframe

        :param body_json: the response body, already decoded as a string
        :param output_names: the names of the outputs to find. If empty, all outputs will be provided
        :return: the dictionary of corresponding dataframes mapped to the output names
        """

        # first read the json as a dictionary
        resultDict = Converters.jsonstr_to_dict(body_json)

        # then transform it into a dataframe
        resultAsDfDict = Collection_Converters.azmltablesdict_to_dfdict(resultDict['Results'], isAzureMlOutput=True)

        # return the expected outputs
        if output_names is None:
            return resultAsDfDict
        else:
            if len(set(output_names) - set(resultAsDfDict.keys())) > 0:
                missings = list(set(output_names) - set(resultAsDfDict.keys()))
                raise Exception(
                    'Error : the following outputs are missing in the results: %s. Found outputs: %s'
                    '' % (missings, set(resultAsDfDict.keys())))
            else:
                slicedDictionary = {k: v for k, v in resultAsDfDict.items() if k in output_names}
            return slicedDictionary


    @staticmethod
    def decode_request_json_body(body_json  # type: str
                                 ):
        # type: (...) -> Tuple[Dict[str, pandas.DataFrame], Dict]
        """
        Reads a request body from a request-response web service call, into a dictionary of pandas dataframe + a
        dictionary of parameters. This is typically useful if you want to debug a request provided by someone else.

        :param body_json:
        :return:
        """

        # first read the json as a dictionary
        resultDict = Converters.jsonstr_to_dict(body_json)

        return Collection_Converters.azmltablesdict_to_dfdict(resultDict['Inputs']), resultDict['GlobalParameters']


class Batch_Client(_BaseHttpClient):
    """ This class provides static methods to call AzureML services in batch mode"""

    def __init__(self,
                 requests_session=None  # type: requests.Session
                 ):
        super(Batch_Client, self).__init__(requests_session=requests_session)

    def push_inputs_to_blob__and__create_output_references(self,
                                                           inputs_df_dict,         # type: Dict[str, pandas.DataFrame]
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

        if output_names is None:
            output_names = []

        # 1- create unique blob naming prefix
        now = datetime.now()
        uniqueBlobNamePrefix = now.strftime("%Y-%m-%d_%H%M%S_%f")

        # 2- store INPUTS and retrieve references
        inputReferences = Collection_Converters.dfdict_to_blobcsvrefdict(inputs_df_dict, blob_service=blob_service,
                                                                         blob_container=blob_container,
                                                                         blob_path_prefix=blob_path_prefix,
                                                                         blob_name_prefix=uniqueBlobNamePrefix + '-input-',
                                                                         charset=charset)

        # 3- create OUTPUT references
        outputReferences = Collection_Converters.create_blob_csv_ref_dict(blob_names=output_names, blob_service=blob_service,
                                                                          blob_container=blob_container,
                                                                          blob_path_prefix=blob_path_prefix,
                                                                          blob_name_prefix=uniqueBlobNamePrefix + '-output-')

        return inputReferences, outputReferences


    def create_request_body(self,
                            input_refs=None,          # type: Dict[str, Dict[str, str]]
                            params_Df_or_Dict=None,
                            output_refs=None          # type: Dict[str, Dict[str, str]]
                            ):
        # type: (...) -> str
        """
        Alias to the static method
        :param input_refs:
        :param params_Df_or_Dict:
        :param output_refs:
        :return:
        """
        return Batch_Client.create_request_body_static(input_refs, params_Df_or_Dict, output_refs)


    @staticmethod
    def create_request_body_static(input_refs=None,         # type: Dict[str, Dict[str, str]]
                                   params_Df_or_Dict=None,
                                   output_refs=None         # type: Dict[str, Dict[str, str]]
                                   ):
        # type: (...) -> str
        """
        Helper method to create a JSON AzureML web service input in Batch mode, from 'by reference' inputs, and parameters as dataframe

        :param input_refs: a dictionary containing input names and input references (each input reference is a dictionary)
        :param params_Df_or_Dict: a dictionary of parameter names and values
        :param output_refs: a dictionary containing output names and output references (each output reference is a dictionary)
        :return: a string representation of the request JSON body (not yet encoded in bytes)
        """

        # params
        if params_Df_or_Dict is None:
            params_Df_or_Dict = {}

        if isinstance(params_Df_or_Dict, dict):
            params = params_Df_or_Dict
        elif isinstance(params_Df_or_Dict, pandas.DataFrame):
            params = Converters.paramdf_to_paramdict(params_Df_or_Dict)
        else:
            raise TypeError(
                'paramsDfOrDict should be a dataframe or a dictionary, or None, found: ' + str(type(params_Df_or_Dict)))

        # final body : combine them into a single dictionary ...
        bodyDict = {'Inputs': input_refs, 'GlobalParameters': params, 'Outputs': output_refs}

        # ... and serialize as Json
        jsonBodyStr = Converters.dict_to_jsonstr(bodyDict)
        return jsonBodyStr

    def execute_batch_createJob(self,
                                base_url,           # type: str
                                api_key,            # type: str
                                request_json_body,  # type: str
                                use_new_ws=False    # type: bool
                                ):
        # type: (...) -> str
        """
        Performs a web service call to AzureML using Batch mode (asynchronous, by reference).
        Supports Fiddler capture for debug.

        :param base_url:
        :param api_key:
        :param request_json_body:
        :param use_new_ws:
        :return:
        """

        batch_url = base_url + '/jobs?api-version=2.0'
        jsonJobId = self._azureml_httpCall(api_key, request_json_body, batch_url, method='POST',
                                                  use_new_ws=use_new_ws)

        # unquote the json Job Id
        if jsonJobId.startswith('"') and jsonJobId.endswith('"'):
            return jsonJobId[1:-1]
        else:
            return jsonJobId

    def execute_batch_startJob(self,
                               base_url,         # type: str
                               api_key,          # type: str
                               job_id,           # type: str
                               use_new_ws=False  # type: bool
                               ):
        """
        Starts an AzureML Batch job (asynchronous, by reference).
        Supports Fiddler capture for debug.

        :param base_url:
        :param api_key:
        :param job_id:
        :param use_new_ws:
        :return:
        """

        batchUrl = base_url + '/jobs/' + job_id + '/start?api-version=2.0'

        self._azureml_httpCall(api_key, None, batchUrl, method='POST', use_new_ws=use_new_ws)
        return

    def execute_batch_getJobStatusOrResult(self,
                                           base_url,          # type: str
                                           api_key,           # type: str
                                           job_id,            # type: str
                                           use_new_ws=False,  # type: bool
                                           ):
        # type: (...) -> str
        """
        Gets the status or the result of an AzureML Batch job (asynchronous, by reference).
        Supports Fiddler capture for debug.

        :param base_url:
        :param api_key:
        :param job_id:
        :param use_new_ws:
        :return:
        """

        batchUrl = base_url + '/jobs/' + job_id + '?api-version=2.0'
        jsonJobStatusOrResult = self._azureml_httpCall(api_key, None, batchUrl, method='GET',
                                                              use_new_ws=use_new_ws)
        return jsonJobStatusOrResult

    def read_status_or_result(self,
                              jobstatus_or_result_json  # type: str
                              ):
        # type: (...) -> Dict[str, Dict[str, str]]
        """
        An alias to the static method

        :param jobstatus_or_result_json:
        :return:
        """
        return Batch_Client.read_status_or_result_static(jobstatus_or_result_json)

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
        resultDict = Converters.jsonstr_to_dict(jobstatus_or_result_json)

        if resultDict['StatusCode'] in ['3','Cancelled']:
            raise IllegalJobStateException('The job state is ' + resultDict['StatusCode'] + ' : cannot read the outcome')

        elif resultDict['StatusCode'] in ['2','Failed']:
            raise JobExecutionException('The job ended with an error : ' + resultDict['Details'])

        elif resultDict['StatusCode'] in ['0','NotStarted','1','Running','4','Finished']:
            jobstatus_or_result_json = resultDict['Results']

        else:
            raise IllegalJobStateException(
                'The job state is ' + resultDict['StatusCode'] + ' : unknown state')

        return jobstatus_or_result_json

    def execute_batch_deleteJob(self,
                                base_url,  # type: str
                                api_key,  # type: str
                                job_id,  # type: str
                                use_new_ws=False,  # type: bool
                                ):
        """
        Deletes an AzureML Batch job (asynchronous, by reference).
        Supports Fiddler capture for debug.

        :param base_url:
        :param api_key:
        :param job_id:
        :param use_new_ws:
        :return:
        """

        batch_url = base_url + '/jobs/' + job_id + '?api-version=2.0'

        self._azureml_httpCall(api_key, None, batch_url, method='DELETE', use_new_ws=use_new_ws)
        return
