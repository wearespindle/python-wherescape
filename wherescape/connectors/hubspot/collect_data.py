import logging
from datetime import datetime
from ...wherescape import WhereScape


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
    if len(result) > 0:
        logging.info(result[0])
        logging.info(len(result))
        logging.info(type(result))
    if len(result) > 1:
        logging.info(result[1])
        # hubspot_process_results(result[1])

# def hubspot_process_results(results):
# # order: hubspot_company_id, client_id, date, user
#     results = 
#     if len(results) == 1:
#         id = 
    
