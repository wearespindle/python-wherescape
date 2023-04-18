import logging
from datetime import datetime
from ...wherescape import WhereScape
from .hubspot_wrapper import Hubspot 


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
    # result = list
    if len(result) > 0:
        logging.info(result[0])
        logging.info(len(result))
        hubspot_process_results(result)
        logging.info("hubspot update done")

def hubspot_process_results(results):
# nrorder        0               1       2     3
# order: hubspot_company_id, client_id, date, user
    hubspot_instance = Hubspot("pat-na1-f92fe637-d403-470e-a39c-329104cb5d75")
    results.pop(0)
    properties = []
    for result in results:
        if len(results) > 4:
            result_dict = {
                'id': result[0],
                "properties": {
                    "users" : result[3]
                }
            }
            properties.append(result_dict)
    if len(properties) > 0:
        hubspot_instance.send_company_patch(properties)
    
