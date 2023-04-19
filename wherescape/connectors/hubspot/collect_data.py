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
    column_names = results.pop(0)
    properties = []
    for result in results:
        # Hubspot only accepts 100 items at a time
        if len(properties) < 100:
            properties.append(process_result(result, column_names))
        else:
            """
            send the collected data in patch, empty properties and start with the next results
            """
            hubspot_instance.send_company_patch(inputs=properties)
            logging.info("sending full batch to Hubspot")
            properties.clear()
            properties.append(process_result(result, column_names))

    if len(properties) > 0:
        hubspot_instance.send_company_patch(inputs=properties)
        logging.info("sending final batch to Hubspot")


# def process_result(result, column_names):
#     """
#     Method that the results of the provided (singular) row into the right setup
#     """
#     result_dict = {"id": result[0], "properties": {"users": result[3]}}
#     logging.info(result_dict)
#     return result_dict


def process_result(result, column_names):
    result_dict = {}
    property_dict = {}

    for name in column_names:
        logging.info("name")
        if name == "hubspot_company_id":
            result_dict["id"] = result[column_names.index(name)]
        elif name == "user_amount":
            property_dict["users"] = result[column_names.index(name)]

    result_dict.update({"properties": property_dict})
    # logging.info(result_dict)

    return result_dict


# def process_result(result, column_names):
#     result_dict = {}
#     property_dict = {}
#     for name in column_names:
#         # hubspot_company_id,client_id, date, user_amount
#         # possible from 3.10
#         match name:
#             case "hubspot_company_id":
#                 result_dict['id'] = result[column_names.index(name)]
#             case "user_amount":
#                 property_dict['users'] = result[column_names.index(name)]
#             case _:     # Default
#                 pass
#     result_dict['properties': property_dict]
#     return result_dict
