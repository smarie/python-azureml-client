import csv
import pandas as pd

from azmlclient.base_databinding import create_dest_buffer_for_csv, create_reading_buffer, azmltable_to_df


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
            input_dfs[input_name] = convert_dct_table_to_df(input_name, dict_table,
                                                            is_timeseries=input_name in timeseries_input_names)

        return input_dfs, global_params_dct


def convert_dct_table_to_df(input_name, dict_table, is_timeseries):
    """
    A fix for Converters.azmltable_to_df

    :param input_name:
    :param dict_table:
    :param is_timeseries:
    :return:
    """
    if not is_timeseries:
        # since the underlying library does not handle it, we artificially add a column.
        if 'ColumnNames' in dict_table.keys() and 'Values' in dict_table.keys():
            values = dict_table['Values']
            if len(values) > 0:
                # use pandas parser to infer most of the types
                # -- for that we first dump in a buffer in a CSV format
                buffer = create_dest_buffer_for_csv()
                writer = csv.writer(buffer, dialect='unix')
                writer.writerows([dict_table['ColumnNames']])
                writer.writerows(values)
                # -- and then we parse with pandas
                res = pd.read_csv(create_reading_buffer(buffer.getvalue()), sep=',', decimal='.')
                buffer.close()
                return res
            else:
                # empty dataframe
                return pd.DataFrame(columns=dict_table['ColumnNames'])
        else:
            raise ValueError(
                'object should be a dictionary with two fields ColumnNames and Values, found: ' + str(
                    dict_table.keys()) + ' for table object: ' + input_name)
    else:
        # there is a datetime as first column, this is ok
        return azmltable_to_df(dict_table, is_azml_output=False, table_name=input_name)
