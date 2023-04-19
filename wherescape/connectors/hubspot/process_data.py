import logging
from .hubspot_wrapper import Hubspot

"""
This module processes the collected data so it can be send to the Hubspot Module
"""


def hubspot_process_results(results, column_names):
    # nrorder        0               1       2     3
    # order: hubspot_company_id, client_id, date, user
    # TODO: find a way to have the access_token obscured. It shouldn't be in the public eye.
    hubspot_instance = Hubspot("pat-na1-f92fe637-d403-470e-a39c-329104cb5d75")
    # column_names = results.pop(0)
    # logging.info(column_names)
    properties = []
    for result in results:
        # Hubspot only accepts 100 items at a time
        if len(properties) < 100:
            properties.append(process_result(result, column_names))
        else:
            """
            send the collected data in patch, empty properties and start with the next results
            """
            logging.info("full batch")
            hubspot_instance.send_company_patch(inputs=properties)
            properties.clear()
            properties.append(process_result(result, column_names))

    if len(properties) > 0:
        logging.info("final batch")
        hubspot_instance.send_company_patch(inputs=properties)


def process_result(result, column_names):
    result_dict = {}
    property_dict = {}

    for name in column_names:
        if name == "hubspot_company_id":
            result_dict["id"] = result[column_names.index(name)]
        elif name == "user_amount":
            property_dict["users"] = result[column_names.index(name)]

    result_dict.update({"properties": property_dict})

    return result_dict
