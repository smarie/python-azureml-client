from abc import abstractmethod, ABCMeta

from valid8 import validate
from six import with_metaclass

try:  # python 3.5+
    from typing import Dict, List
except ImportError:
    pass

from requests import Session
import pandas as pd

from azmlclient.base import execute_bes, execute_rr
from azmlclient.clients_config import ServiceConfig


class CallMode(with_metaclass(ABCMeta, object)):
    """
    Abstract class representing a call mode
    """
    pass


class LocalCallMode(CallMode):
    """
    A "local" call mode
    """
    pass


class RemoteCallMode(CallMode):
    """
    Represents a way to call a service. It is composed of a main mode, and various options
    """
    @abstractmethod
    def call_azureml(self,
                     service_id,            # type: str
                     service_config,        # type: ServiceConfig
                     ws_inputs,             # type: Dict[str, pd.DataFrame]
                     ws_params=None,        # type: Dict[str, str]
                     ws_output_names=None,  # type: List[str]
                     session=None,          # type: Session
                     by_ref_inputs=None,    # type: Dict[str, str]
                     **kwargs
                     ):
        # type: (...) -> Dict[str, pd.DataFrame]
        """
        This method is called by `AzureMLClient` instances when their current call mode is a remote call mode.

        :param service_id: the service id that will be used in error messages
        :param service_config:
        :param ws_inputs:
        :param ws_params:
        :param ws_output_names:
        :param session:
        :param by_ref_inputs:
        :param kwargs:
        :return:
        """
        pass


class RequestResponse(RemoteCallMode):
    """
    Represents the request-response call mode
    """

    # noinspection PyMethodOverriding
    def call_azureml(self,
                     service_id,            # type: str
                     service_config,        # type: ServiceConfig
                     ws_inputs,             # type: Dict[str, pd.DataFrame]
                     ws_params=None,        # type: Dict[str, str]
                     ws_output_names=None,  # type: List[str]
                     session=None,          # type: Session
                     ):
        # type: (...) -> Dict[str, pd.DataFrame]
        """
        (See super for description)
        """
        validate("%s:base_url" % service_id, service_config.base_url)
        validate("%s:api_key" % service_id, service_config.api_key)

        # standard azureml request-response call
        return execute_rr(api_key=service_config.api_key, base_url=service_config.base_url,
                          inputs=ws_inputs, params=ws_params, output_names=ws_output_names,
                          requests_session=session)


class Batch(RemoteCallMode):
    """
    Represents the "Batch" call mode.
    """
    def __init__(self,
                 polling_period_seconds=5  # type: int
                 ):
        self.polling_period_seconds = polling_period_seconds

    # noinspection PyMethodOverriding
    def call_azureml(self,
                     service_id,            # type: str
                     service_config,        # type: ServiceConfig
                     ws_inputs,             # type: Dict[str, pd.DataFrame]
                     ws_params=None,        # type: Dict[str, str]
                     ws_output_names=None,  # type: List[str]
                     session=None,          # type: Session
                     ):
        """
        (See super for base description)
        :return:
        """
        validate("%s:base_url" % service_id, service_config.base_url)
        validate("%s:api_key" % service_id, service_config.api_key)
        validate("%s:blob_account" % service_id, service_config.blob_account)
        validate("%s:blob_api_key" % service_id, service_config.blob_api_key)
        validate("%s:blob_container" % service_id, service_config.blob_container)

        return execute_bes(
            # all of this is filled using the `service_config`
            api_key=service_config.api_key, base_url=service_config.base_url,
            blob_storage_account=service_config.blob_account, blob_storage_apikey=service_config.blob_api_key,
            blob_container=service_config.blob_container, blob_path_prefix=service_config.blob_path_prefix,
            # blob_charset=None,
            # -------
            inputs=ws_inputs, params=ws_params, output_names=ws_output_names,
            nb_seconds_between_status_queries=self.polling_period_seconds,
            requests_session=session
        )


# class RequestResponseInputsByRef(RequestResponse):
#     """
#     Represents the "Request Response" call mode with additional capability to pass some of the inputs "by reference".
#     Note that the web service has to be designed accordingly.
#     """
#     # noinspection PyMethodOverriding
#     def call_azureml(self,
#                      service_config,        # type: ServiceConfig
#                      ws_inputs,             # type: Dict[str, pd.DataFrame]
#                      ws_params=None,        # type: Dict[str, str]
#                      ws_output_names=None,  # type: List[str]
#                      session=None,          # type: Session
#                      by_ref_inputs=None,    # type: Dict[str, str]
#                      ):
#         # type: (...) -> Dict[str, pd.DataFrame]
#         """
#         (See super for base description)
#
#         :param by_ref_inputs: a dictionary {<input_name>: <param_name>} containing one entry for each input to send
#             "by reference" rather than "by value". Each such input will be removed from the service inputs (the names
#             have to be valid input names), its contents will be stored in the blob storage (the same used for batch
#             mode), and the blob URL will be passed to a new parameter named <param_name>
#         :return:
#         """
#         # by reference: we have to upload some inputs to the blob storage first
#         if by_ref_inputs is None:
#             by_ref_inputs = dict()
#
#         # copy inputs and params since we will modify them
#         ws_inputs = copy(ws_inputs)
#         ws_params = copy(ws_params)
#
#         for by_ref_input_name, by_ref_refparam_name in by_ref_inputs.items():
#             # -- push input in blob and get a reference
#             input_to_be_ref = ws_inputs.pop(by_ref_input_name)
#             sas_url = push_blob_and_get_ref(input_to_be_ref, service_config=service_config, session=session)
#
#             # -- create the new param containing the reference
#             ws_params[by_ref_refparam_name] = sas_url
#
#         # -- execute Request-Response on alternate 'by ref' endpoint
#         return execute_rr(api_key=service_config.rr_by_ref_api_key,
#                           base_url=service_config.rr_by_ref_base_url,
#                           inputs=ws_inputs, params=ws_params, output_names=ws_output_names,
#                           requests_session=session)
#
#
# def push_blob_and_get_ref(input,
#                           service_config: ServiceConfig,
#                           session: Session):
#     """
#     Uploads input to the blob storage defined in service_config (blob_account_for_batch, blob_apikey_for_batch).
#     Generates a temporary shared access key valid for two hours, and returns the corresponding blob URL.
#
#     :param input:
#     :param service_config:
#     :param session:
#     :return:
#     """
#     # a dummy name used only in this method
#     by_ref_input_name = 'foo'
#
#     # -- first upload the input to a blob storage and get the absolute reference to it.
#     blob_service = BlockBlobService(account_name=service_config.blob_account_for_batch,
#                                     account_key=service_config.blob_apikey_for_batch,
#                                     request_session=session)
#     batch_client = BatchClient(requests_session=session)
#     input_refs, output_refs = batch_client. \
#         push_inputs_to_blob__and__create_output_references({by_ref_input_name: input},
#                                                            output_names=[],
#                                                            blob_service=blob_service,
#                                                            blob_container=service_config.blob_containername_for_batch,
#                                                            blob_path_prefix='')
#
#     # -- then generate shared access key (public SAS access to blob)
#     blob_relative_loc = input_refs[by_ref_input_name]['RelativeLocation']
#     blob_name = blob_relative_loc[blob_relative_loc.find('/') + 1:]
#     expiry_date = datetime.now() + timedelta(hours=2)  # expires in 2 hours
#     sas_token = blob_service.generate_blob_shared_access_signature(
#         container_name=service_config.blob_containername_for_batch,
#         blob_name=blob_name, expiry=expiry_date,
#         permission=BlobPermissions.READ)
#     sas_url = blob_service.make_blob_url(container_name=service_config.blob_containername_for_batch,
#                                          blob_name=blob_name, sas_token=sas_token)
#     return sas_url
