import logging
from datetime import datetime
from wherescape import WhereScape

# def wherescape_instance():
#     start_time = datetime.now()
#     logging.info("connecting to WhereScape")
#     wherescape_instance = WhereScape()
#     logging.info(
#         "Start time: %s for hubspot_load_data" % start_time.strftime("%Y-%m-%d %H:%M:%S")
#     )
#     logging.info("post load")
#     return wherescape_instance

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


# def hubspot_load_fact_ps_user():

#     ori_sql = "SELECT fu.client_id, fu.user_amount, fu.user_addition, fu.user_subtraction, COALESCE(fu.user_addition, 0) + COALESCE(fu.user_subtraction, 0) AS user_change, dhcv.hubspot_company_id FROM star.fact_ps_user fu INNER JOIN star.dim_hubspot_company_voys dhcv ON dhcv.voipgrid_pk = fu.client_id WHERE fu.'date'= CURRENT_DATE ORDER BY user_amount DESC"
#     wherescape_instance = WhereScape()
#     result = wherescape_instance.query_target(ori_sql)