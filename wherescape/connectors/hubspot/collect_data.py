import logging
from datetime import datetime
from ...wherescape import WhereScape


def hubspot_load_data():
    start_time = datetime.now()
    logging.info("connecting to WhereScape")
    wherescape_instance = WhereScape()
    logging.info(
        "Start time: %s for hubspot_load_data" % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )
    logging.info("post load")
    table_name = wherescape_instance.load_full_name
    logging.info(table_name)
    sql = f"select * from {table_name}"
    result = wherescape_instance.query_target(sql)
    if len(result) > 0:
        logging.info(result[0])


# import logging
# from wherescape import WhereScape


# wherescape_instance = WhereScape()
# logging.info("post load")
# table_name = wherescape_instance.load_full_name
# logging.info(table_name)
# sql = f"select * from {table_name}"
# result = wherescape_instance.query_target(sql)
# if len(result) > 0:
#     logging.info(result[0])
