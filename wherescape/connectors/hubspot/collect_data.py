import logging
from datetime import datetime
from ...wherescape import WhereScape
from .process_data import hubspot_process_results

""" 
this module retrieves the data from Wherescape
"""
api_key = "pat-na1-f92fe637-d403-470e-a39c-329104cb5d75"


def hubspot_load_data():
    start_time = datetime.now()
    logging.info("connecting to WhereScape")
    wherescape_instance = WhereScape()
    logging.info(
        "Start time: %s for hubspot_load_data"
        % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )
    logging.info("post load")
    table_name = f"{wherescape_instance.schema}.{wherescape_instance.table}"
    logging.info(table_name)
    sql = f"select * from {table_name}"
    result = wherescape_instance.query_target(sql)

    column_names = wherescape_instance.get_columns()[0]

    if len(result) > 0:
        logging.info(result[0])
        logging.info(len(result))
        hubspot_process_results(api_key, result, column_names)
        logging.info("hubspot update done")
