import logging
from .hubspot_wrapper import Hubspot

"""
This module processes the collected data so it can be send to the Hubspot Module
"""


# TODO: check posibilities to separate sending it to hubspot
# TODO: find a way to have the access_token obscured. It shouldn't be in the public eye.


def hubspot_process_results(results, column_names):
    """
    method to process results to Hubspot
    """
    hubspot_instance = Hubspot("pat-na1-f92fe637-d403-470e-a39c-329104cb5d75")
    properties = []
    for result in results:
        # Hubspot only accepts 100 items at a time
        if len(properties) < 100:
            properties.append(result_to_dict(result, column_names))
        else:
            """
            send the collected data in patch, empty properties and start with the next results
            """
            logging.info("full batch")
            hubspot_instance.send_company_patch(inputs=properties)
            properties.clear()
            properties.append(result_to_dict(result, column_names))

    if len(properties) > 0:
        logging.info("final batch")
        hubspot_instance.send_company_patch(inputs=properties)


def result_to_dict(result, column_names):
    result_dict = {}
    property_dict = {}

    for name in column_names:
        if name == "hubspot_company_id":
            result_dict["id"] = result[column_names.index(name)]
        elif name == "user_amount":
            property_dict["users"] = result[column_names.index(name)]

    result_dict.update({"properties": property_dict})

    return result_dict


# NOTE: This only works from Python version 3.10 and newer
# def to_dict(result, column_names):
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
