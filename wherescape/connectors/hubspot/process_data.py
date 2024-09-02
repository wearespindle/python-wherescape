import logging
from decimal import Decimal

from .hubspot_wrapper import Hubspot


"""
This module processes the collected data so it can be send to the Hubspot Module.
"""


def hubspot_process_results(
    access_token: str, results: list, column_names: list, table_name: str
):
    """
    function that handles the processing of the results for it to be send to Hubspot
    Function to process the results to be send to hubspot.

    Parameters:
    - access_token (string): token connecting to the private app allowing access to hubspot
    - results (list): content that will be sent to hubspot
    - column_names (list): names related to the data to know what data goes in which hubspot property
    - table_name (string): name of the table containing info about the desired process and destination
    """
    hubspot_instance = Hubspot(access_token)
    properties = []

    object_name = get_object_name(table_name)
    property_names = hubspot_instance.get_property_names(object_name)
    overlapping_names = compare_names(column_names, property_names)

    """
    Loops through results. Per row, the data is put into a dict after which this dict is added to the properties list.
    Once the lists reaches 100 items (limit set by HubSpot's API), those get send as batch and the list is emptied for a new batch.
    """
    for result in results:
        """hubspot only accepts 100 items per call"""
        if len(properties) < 100:
            properties.append(create_data_dict(result, column_names, overlapping_names))
        else:
            """
            send the collected data in patch, empty properties and start with the next results
            """
            logging.info("full batch ready")
            send_data_to_hubspot(object_name, properties, hubspot_instance)

            properties.clear()
            properties.append(create_data_dict(result, column_names, overlapping_names))

    if len(properties) > 0:
        logging.info("final batch ready")
        send_data_to_hubspot(object_name, properties, hubspot_instance)


def send_data_to_hubspot(object_type: str, properties: list, hubspot_instance: Hubspot):
    """
    Function to send data as the correct request. Currently only as Patch so no checks are done.

    Parameters:
    - object_type (string): refers to hubspot objects
    - properties (list): list of properties that will be updated
    - hubspot_instance (Hubspot): HubSpot environment data will be sent to
    """
    hubspot_instance.update_batch(properties, object_type)


def create_data_dict(result: list, column_names: list, known_names: list):
    """
    This Function processes a list of results into a dict to fit the needs and 
    expectations from HubSpot.

    Parameters:
    - result (list) : list data from the WhereScape being table used
    - column_names (list) : list of the column_names from the WhereScape table being used
    - known_names (list) : list of column_names that also exist as property in the HubSpot object the data will go to

    Returns:
    - result_dict (dict) : dict with the data containing all the properties with its data organised using the hs_object_id's
    """
    result_dict = {}
    property_dict = {}

    for name in column_names:
        data_item = result[column_names.index(name)]
        if name in known_names:
            """
            For 1-to-1 column_names and property_names
            """

            if name == "hs_object_id":
                """hs_object_id has to be an int"""
                result_dict["id"] = int(data_item)
            else:
                if isinstance(data_item, Decimal):
                    data_item = float(data_item)

                property_dict[name] = data_item

    result_dict.update({"properties": property_dict})
    return result_dict


def compare_names(source_names: list, destination_names: list):
    """
    This function compares source names with destiny names for one to one data transfer.
    It will give a warning for source names that do not appear in the list of destiny names.

    Parameters:
    - source_names (list) : list of column_names from the source table being used
    - destination_names (list): list of all property names of the HubSpot object the data will go to

    Return:
    - known_destination_names (list) : list of property names that also exist as column names in the source table being used
    """
    known_destination_names = []
    unknown_destination_names = []

    for name in source_names:
        if name not in destination_names and name != "record_id" and "dss_" not in name:
            unknown_destination_names.append(name)
        elif "dss_" in name:
            # These are generated columsn for during the modeling process.
            pass
        else:
            known_destination_names.append(name)
    if len(unknown_destination_names) > 0:
        logging.warning(f"following properties were not located in destination {unknown_destination_names}")

    return known_destination_names


def get_object_name(table_name: str):
    """
    This function will return the name of the object the data will be sent to.

    Parameters:
    - table_name (string) : name of the source table

    Returns:
    - (string): name used to refer to the destination hubspot object
    """
    if "companies" in table_name or "company" in table_name:
        return "companies"
    elif "contacts" in table_name or "contact" in table_name:
        return "contacts"
    elif "deals" in table_name or "deal" in table_name:
        return "deals"
    else:
        logging.error(
            "Could not identify the specific hubspot object type based of the table name."
        )
