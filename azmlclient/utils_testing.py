from azmlclient.base_databinding import azmltable_to_df


class AzuremlWebServiceMock(object):
    """
    A mock of azureML exposing web services, compliant with CherryPY
    """
    name = None

    def GET(self):
        return "Hello World! This is service " + self.name

    def unpack_azureml_query(self,
                             json_dct,
                             input_names=(),
                             swagger_mode=None,  # type: bool
                             ):
        """

        :param json_dct:
        :param input_names:
        :param swagger_mode:
        :return:
        """
        # support single string input. convert to tuple in that case
        if isinstance(input_names, str):
            input_names = (input_names, )

        inputs_dct = json_dct['Inputs']
        global_params_dct = json_dct['GlobalParameters']

        # return CollectionConverters.azmltables_to_dfs(json_dct['Inputs']),
        input_dfs = dict()
        for input_name, dict_table in inputs_dct.items():
            if input_name not in input_names:
                raise ValueError("Invalid input received. Supported inputs are: %s" % input_names)

            input_dfs[input_name] = azmltable_to_df(dict_table, is_azml_output=False, table_name=input_name,
                                                    swagger_mode=swagger_mode)

        return input_dfs, global_params_dct
