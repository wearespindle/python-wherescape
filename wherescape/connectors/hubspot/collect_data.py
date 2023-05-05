import logging
from datetime import datetime
from ...wherescape import WhereScape
from .process_data import hubspot_process_results

""" 
this module retrieves the data from Wherescape
"""

"""
this is a list of all environments in HubSpot in case someone uses multople
"""


def hubspot_load_data():
    """
    This method collects all the data from a table and sends it to be processed.
    """
    start_time = datetime.now()
    logging.info("connecting to WhereScape")
    wherescape_instance = WhereScape()
    logging.info(
        "Start time: %s for hubspot_load_data"
        % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )
    logging.info("post load")
    table_name = f"{wherescape_instance.schema}.{wherescape_instance.table}"
    sql = f"select * from {table_name}"

    result = wherescape_instance.query_target(sql)
    access_token = hubspot_get_token(wherescape_instance, table_name)
    column_names = wherescape_instance.get_columns()[0]

    if len(result) > 0:
        hubspot_process_results(access_token, result, column_names, table_name)
        logging.info("hubspot update done")


def hubspot_get_token(wherescape_instance: WhereScape, table_name: str):
    parameter_name = "hubspot_access_token_test_environment"
    table_split = table_name.split("_")
    table_words = list(table_split)[5, len(table_split)]
    logging.info(table_words)

    for word in table_words:
        environment_parameter = parameter_name + "_" + word
        access_token = wherescape_instance.read_parameter(environment_parameter)
        if access_token:
            return access_token
    return wherescape_instance.read_parameter(parameter_name)
