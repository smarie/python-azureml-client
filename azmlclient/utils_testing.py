from azmlclient.base_databinding import azmltable_to_df


class AzuremlWebServiceMock(object):
    """
    A mock of azureML exposing web services, compliant with CherryPY
    """
    name = None

    def GET(self):
        return "Hello World! This is service " + self.name

    def unpack_azureml_query(self, json_dct, timeseries_input_names=(), table_input_names=()):
        """

        :param json_dct:
        :return:
        """
        # support single string input. convert to tuple in that case
        if isinstance(timeseries_input_names, str):
            timeseries_input_names = (timeseries_input_names, )

        if isinstance(table_input_names, str):
            table_input_names = (table_input_names, )

        inputs_dct = json_dct['Inputs']
        global_params_dct = json_dct['GlobalParameters']

        # return CollectionConverters.azmltables_to_dfs(json_dct['Inputs']),
        input_dfs = dict()
        for input_name, dict_table in inputs_dct.items():
            if input_name not in timeseries_input_names and input_name not in table_input_names:
                raise ValueError("Invalid input received. Supported inputs are: %s and %s" % (timeseries_input_names,
                                                                                              table_input_names))
            input_dfs[input_name] = azmltable_to_df(dict_table, is_azml_output=False, table_name=input_name)

        return input_dfs, global_params_dct
