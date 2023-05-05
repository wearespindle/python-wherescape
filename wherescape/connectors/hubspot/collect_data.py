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
    access_token = wherescape_instance.read_parameter(
        "hubspot_access_token_test_environment"
    )
    column_names = wherescape_instance.get_columns()[0]

    if len(result) > 0:
        hubspot_process_results(access_token, result, column_names, table_name)
        logging.info("hubspot update done")
