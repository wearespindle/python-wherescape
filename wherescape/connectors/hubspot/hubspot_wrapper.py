import logging
from enum import StrEnum, auto

import hubspot.crm
from hubspot.client import Client
from hubspot.crm import AssociationType, associations, companies, contacts, deals, properties, tickets

from ...helper_functions import is_date


"""
Module that takes care of HubSpot connection and API calls to HubSpot.
for information Hubspot requests, go to https://developers.hubspot.com/docs/

global parameters:
- batch_input_map (map) map of the batch_inputs referring to the different classes designed for the different HubSpot classes
"""

class HubspotObjectEnum(StrEnum):
    COMPANIES = auto()
    CONTACTS = auto()
    DEALS = auto()
    TICKETS = auto()


batch_input_map = {
    HubspotObjectEnum.COMPANIES: companies.BatchInputSimplePublicObjectBatchInput,
    HubspotObjectEnum.CONTACTS: contacts.BatchInputSimplePublicObjectBatchInput,
    HubspotObjectEnum.DEALS: deals.BatchInputSimplePublicObjectBatchInput,
    HubspotObjectEnum.TICKETS: tickets.BatchInputSimplePublicObjectBatchInput,
}


class Hubspot:
    def __init__(self, access_token: str):
        """
        Set up Hubspot connection.
        """
        try:
            self.client: Client = Client.create(access_token=access_token)
        except Exception:
            logging.error("The connection with HubSpot failed. Please Check if the access token is still correct.")


    def send_patch(self, properties: list, hs_object: str):
        """
        Function that updates properties for a batch of the given hs_object.

        Parameters:
        - properties: data to be send
        - hs_object (string): hubspot object data will be sent to
        """
        logging.info("sending %s batch patch to hubspot" % hs_object)

        batch_input_class = get_batch_input_class(hs_object)
        batch_input = batch_input_class(inputs=properties)
        try:
            batch_api = getattr(self.client.crm, hs_object).batch_api
            error_api = getattr(hubspot.crm, HubspotObjectEnum(hs_object))
            response = batch_api.update(
                batch_input_simple_public_object_batch_input=batch_input
            )
        except error_api.ApiException as e:
            logging.error("Exception when calling batch_api->update: %s\n" % e)

        try:
            response.errors
            errors = response.errors
            if len(errors) > 0:
                log_errors(errors)

        except Exception:
            pass

    def update_batch(self, object_items: list, hs_object:str):
        """
        Method that updates a batch of items for a Hubspot object.

        Params:
        - object_items (list): list of hubspot items to be updated.
        - hs_object (str): name of the hs_object to be updated
        """
        input_list = update_properties_list(object_items)
        input_batch_class = get_batch_input_class(hs_object)
        input_batch = input_batch_class(input_list)

        try:
            api_batch = getattr(self.client.crm, HubspotObjectEnum(hs_object)).batch_api
            api_error = getattr(hubspot.crm, HubspotObjectEnum(hs_object))

            api_batch.update(input_batch)
        except api_error.ApiException as e:
            logging.error(f"Exception when calling {hs_object} batch_api->update\n {e}")

    def get_properties(self, object_name: str):
        """
        Function to get the properties of an object.

        Paramters:
        - object_name (string): name of the hubspot object the properties need to come from

        Returns
        - property_names (list): list of all the propertynames under an object
        """
        property_names = []
        try:
            api_response = self.client.crm.properties.core_api.get_all(
                object_type=object_name, archived=False
            )
            api_results = api_response.to_dict()

            for result in api_results["results"]:
                property_names.append(result["name"])

            return property_names
        except properties.ApiException as e:
            logging.error("Exception when calling core_api->get_all: %s\n" % e)

    def get_all(
        self,
        hs_object: str,
        properties: list = [],
    ):
        """
        Method to retrieve all items of a hubspot object.

        Params:
        - hs_object (str): name of the hubspot object
        - properties (list): list of properties desired to retreieve. Empty by default
        """
        try:
            results = []
            basic_api = getattr(self.client.crm, hs_object).basic_api
            error_api = getattr(hubspot.crm, HubspotObjectEnum(hs_object))
            api_response = basic_api.get_page(properties=properties, limit=100)

            results.extend(api_response.results)
            count = 0

            while api_response.paging:
                api_response = basic_api.get_page(
                    properties=properties,
                    limit=100,
                    after=api_response.paging.next.after,
                )

                results.extend(api_response.results)
                count += 1
                print(count)

        except error_api.ApiException as e:
            logging.error(e)

        logging.info(f"returning {len(results)} items of Hubspot {hs_object}.")
        return results

    def merge_tickets(self, ticket_a, ticket_b) -> tuple:
        """
        This method merges the properties into tickets into the ticket that is the oldest.

        Params:
        - ticket_a : hubspot ticket to be merged
        - ticket_b : hubspot ticket to be merged

        Returns:
        - to_keep: ticket to be kept and updated in Hubspot
        - to_remove: ticket which can be removed after the merge has succeeded with Hubspot.
        """
        # We keep the newer ticket and merge what is needed from the older ticket.
        to_remove = ticket_a if ticket_a.created_at < ticket_b.created_at else ticket_b
        to_keep = ticket_b if ticket_a.created_at < ticket_b.created_at else ticket_a

        for property_ in to_remove.properties:
            if to_remove.properties[property_] is not None and to_keep.properties[property_] is not None:
                if property_.startswith("hs_") or is_date(to_keep.properties[property_]):
                    pass # Ignoring dates and hubspot owned properties (set by hubspot).
                elif to_remove.properties[property_] == to_keep.properties[property_]:
                    pass # No change needed if the properties are the same.
                elif property_ == "content":
                    # For content, we add them together to keep all content.
                    to_keep.properties[property_] += to_remove.properties[property_]
            else:
                if to_keep.properties[property_] is None and to_remove.properties[property_] is not None:
                    to_keep.properties[property_] = to_remove.properties[property_]
        # Move any associations of the to_remove ticket to the to_keep ticket.
        if to_remove.associations is not None:
            for key in to_remove.associations:
                associations = to_remove.associations[key]
                for association in associations.results:
                    self.update_association(association, key, to_keep, "tickets")

        return to_keep, to_remove

    def update_association(self, association, association_type, object_, object_type):
        """
        Method to add association of an object to a new object using existing connection type.

        Params:
        - association: Hubspot Association object.
        - association_type (str): type of association
        """
        association_type_id = getattr(AssociationType, association.type.upper())
        association_id = association.id
        object_id= object_.id

        try:
            self.client.crm.associations.v4.basic_api.create(
                object_type=object_type,
                object_id=object_id,
                to_object_type=association_type,
                to_object_id= association_id,
                association_spec=[{
                    "associationCategory": "HUBSPOT_DEFINED",
                    "associationTypeId": association_type_id,
                }],
            )
        except associations.ApiException as e:
            logging.error(f"Exception when calling batch_apo->create: {e}")

    def archive_object(self, object_id: str, hs_object: str):
        """
        Funtion to archive one object.

        Parameters:
            object_id (str): id of the object deleted.
            object_type (str): husbpot object type,
        """
        logging.info(f"Archiving {hs_object} object with record_id {object_id}.")

        try:
            basic_api = getattr(self.client.crm, HubspotObjectEnum(hs_object)).basic_api
            error_api = getattr(hubspot.crm, HubspotObjectEnum(hs_object))
            basic_api.archive(object_id)
        except error_api.ApiException as e:
            logging.error(f"Exception when calling basic_api->archive: {e}")

    def batch_archive(self, object_ids: list, hs_object:str):
        """
        Funtion to archive one object.

        Parameters:
            object_ids (list): list of of dicts containing {"id": string} to be deleted.
            object_type (str): husbpot object type,
        """
        logging.info(f"Archiving {len(object_ids)} items from {hs_object}.")
        input_batch_class = get_batch_input_class(hs_object)
        input_batch = input_batch_class(object_ids)

        try:
            batch_api = getattr(self.client.crm, HubspotObjectEnum(hs_object)).batch_api
            error_api = getattr(hubspot.crm, HubspotObjectEnum(hs_object))

            batch_api.archive(input_batch)
        except error_api.ApiException as e:
            logging.error(f"Exception when calling basic_api->archive: {e}")


def log_errors(errors):
    """
    Function for the logging of errors in the response of the API request.

    Parameters:
    - errors: errors obtained from response

    """
    for error in errors:
        category = error.category
        context_ids = ", ".join(error.context["ids"])
        """
        The process was stopped
        """
        logging.error(
            f"The process was stopped prematurely resulting from an error of category {category} with record_ids: {context_ids} "
        )

def get_batch_input_class(hs_object:str):
    """
    Method to check if object exists in batch_input_map.
    Logs as error if no input class was found.

    Params:
    - hs_object (str): name of the object type
    """
    batch_input_class = batch_input_map.get(HubspotObjectEnum(hs_object))
    if not batch_input_class:
        logging.error("Invalid hs_object: %s" % hs_object)
    return batch_input_class

def update_properties_list(hubspot_items: list) -> list:
    """
    Function to set a collection of hubspot objects into a correct list for updating properties.

    Params:
    - hubspot_items (list): list of all the hubspot_objects that need to be updated.

    Returns:
    - list of all items ready to be sent to Hubspot
    """
    final_list = []
    for item in hubspot_items:
        properties = item.properties
        change_properties = {}
        for prop in properties:
            # not changing properties set by hubspot or the create date.
            if not prop.startswith("hs_") and prop != "createdate":
                change_properties[prop] = properties[prop]
        item_dict = {
            "id": item.id,
            "properties": change_properties,
        }
        final_list.append(item_dict)

    return final_list
