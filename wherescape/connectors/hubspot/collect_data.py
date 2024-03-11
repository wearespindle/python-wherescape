import logging
from datetime import datetime

from ...wherescape import WhereScape
from .process_data import hubspot_process_results

""" 
this module retrieves the data from Wherescape
"""


def hubspot_load_data():
    """
    Function to load data from table and send to be processed
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

    # Determine whether it's run in development or production.
    environment = wherescape_instance.meta_db_connection_string.split(";")[0]
    develop_env = True if "dev" in environment.lower() else False

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
    Function to get the hubspot access token from the table

    Parameters:
    - wherescape_instance (WhereScape): the wherescape database instance to connect to.
    - table_name (string): name of the wherescape table data is send from

    Returns
    - access_token (string): the token to be able to connect to hubspot.
    """
    parameter_name = "hubspot_access_token"
    table_words = table_name.split("_")

    logging.info("retrieving access_token")
    if develop_env:
        logging.info("using developmental environment")
        parameter_name = parameter_name + "_sandbox"
        return wherescape_instance.read_parameter(parameter_name)

    for word in table_words:
        environment_parameter = parameter_name + "_" + word
        access_token = wherescape_instance.read_parameter(environment_parameter)

        if access_token:
            logging.info("retreived acces token from %s" % environment_parameter)
            return access_token

    logging.warn("no specified environment found")
    logging.info("retrieving access token from parameter %s" % parameter_name)
    return wherescape_instance.read_parameter(parameter_name)
