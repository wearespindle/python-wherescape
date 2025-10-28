import logging
from datetime import datetime

from ...wherescape import WhereScape
from .process_data import hubspot_process_results


"""
This module uses the Wherescape environment to update hubspot objects.
"""


def hubspot_load_data():
    """
    Function to load data from table and send to be processed.
    """
    start_time = datetime.now()
    logging.info("connecting to WhereScape")
    wherescape_instance = WhereScape()
    logging.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')} for hubspot_load_data")
    logging.info("post load")
    table_name = f"{wherescape_instance.schema}.{wherescape_instance.table}"
    sql = f"select * from {table_name}"

    # Determine whether it's run in development or production.
    environment = wherescape_instance.meta_db_connection_string.split(";")[0]
    develop_env = "dev" in environment.lower()

    result = wherescape_instance.query_target(sql)
    access_token = hubspot_get_token(wherescape_instance, table_name, develop_env)
    column_names = wherescape_instance.get_columns()[0]

    if len(result) > 0:
        hubspot_process_results(access_token, result, column_names, table_name)
        logging.info("hubspot update done")


def hubspot_get_token(
    wherescape_instance: WhereScape, table_name: str, develop_env: bool
):
    """
    Function to get the hubspot access token from the table.
    First trying with environemnt specification from table_name.
    Second checking for dev environment.
    Last using base name.

    Parameters:
    - wherescape_instance (WhereScape): the wherescape database instance to connect to.
    - table_name (string): name of the wherescape table data is send from

    Returns
    - access_token (string): the token to be able to connect to hubspot.
    """
    parameter_name = "hubspot_access_token"
    table_words = table_name.split("_")

    logging.info("retrieving access_token")

    # return access token if it can be found with words in table
    for word in table_words:
        environment_parameter = parameter_name + "_" + word

        if develop_env:
            environment_parameter = environment_parameter + "_dev"
            access_token = wherescape_instance.read_parameter(environment_parameter)
            if access_token:
                return access_token

        access_token = wherescape_instance.read_parameter(environment_parameter)
        if access_token:
            logging.info(f"retreived acces token from {environment_parameter}")
            return access_token

    # return acces token sandbox if environment is development
    if develop_env:
        logging.info("using developmental environment")
        parameter_name = parameter_name + "_dev"
    else:
        logging.info("no specified environment found")
    logging.info(f"retrieving access token from parameter {parameter_name}")
    # return access token on base name
    return wherescape_instance.read_parameter(parameter_name)
