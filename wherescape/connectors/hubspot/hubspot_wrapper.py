import logging
from enum import StrEnum, auto
from time import sleep

import hubspot.crm
from hubspot.client import Client
from hubspot.crm import (
    AssociationType, 
    associations, 
    companies, 
    contacts, 
    deals, 
    properties, 
    tickets,
)

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
simple_search_map = {
    HubspotObjectEnum.COMPANIES: companies.PublicObjectSearchRequest,
    HubspotObjectEnum.CONTACTS: contacts.PublicObjectSearchRequest,
    HubspotObjectEnum.DEALS: deals.PublicObjectSearchRequest,
    HubspotObjectEnum.TICKETS: tickets.PublicObjectSearchRequest,
}


class Hubspot:
    def __init__(self, access_token: str):
        """
        Set up Hubspot connection.
        """
        try:
            self.client: Client = Client.create(access_token=access_token)
        except Exception:
            logging.error("The connection with HubSpot failed. Please Check if the access token is still up to date.")
            exit()

    def send_patch(self, properties: list, hs_object: str):
        """
        Function that updates properties for a batch of the given hs_object.

        Parameters:
        - properties: data to be send
        - hs_object (string): hubspot object data will be sent to
        """
        logging.info(f"sending {hs_object} batch patch to hubspot")

        batch_input_class = get_batch_input_class(hs_object)
        batch_input = batch_input_class(inputs=properties)
        batch_api = getattr(self.client.crm, hs_object).batch_api
        error_api = getattr(hubspot.crm, HubspotObjectEnum(hs_object))
        try:
            response = batch_api.update(
                batch_input_simple_public_object_batch_input=batch_input
            )
        except error_api.ApiException as e:
            logging.error(f"Exception when calling batch_api->update: {e}\n")
            return None

        try:
            response.errors
            errors = response.errors
            if len(errors) > 0:
                log_errors(errors)
        except Exception:
            pass
        return response

    def update_batch(self, object_items: list, hs_object:str):
        """
        Method that updates a batch of items for a Hubspot object.

        Params:
        - object_items (list): list of hubspot items to be updated.
        - hs_object (str): name of the hs_object to be updated
        """
        input_list = update_properties_list(object_items)
        input_batch_class = get_batch_input_class(hs_object)
        response = []
        api_batch = getattr(self.client.crm, HubspotObjectEnum(hs_object)).batch_api
        api_error = getattr(hubspot.crm, HubspotObjectEnum(hs_object))

        # While more than 100, do 100 at a time.
        if len(object_items) > 100:
            while len(object_items) > 100:
                input_batch = input_batch_class(input_list[100:])
                try:
                    response.append(api_batch.update(input_batch))
                except api_error.ApiException as e:
                    logging.error(f"Exception when calling {hs_object} batch_api->update\n {e}")
                    return None #stopping the program
                del object_items[:100]
        # When less than 100, do all in one go.
        try:
            input_batch = input_batch_class(input_list)
            api_batch = getattr(self.client.crm, HubspotObjectEnum(hs_object)).batch_api
            api_error = getattr(hubspot.crm, HubspotObjectEnum(hs_object))

            api_batch.update(input_batch)
        except api_error.ApiException as e:
            logging.error(f"Exception when calling {hs_object} batch_api->update\n {e}")

    def get_object(self, record_id: str, hs_object: str, properties: list = []):
        """
        Method that retreives the specified object.

        Params:
        - record_id(str): unique id used to locate item in Hubspot.
        - hs_object(str): type of the object.
        - properties(list): optional. list of properties retreived with the object.

        Returns:
        - Hubspot object 
        """
        basic_api = getattr(self.client.crm, HubspotObjectEnum(hs_object)).basic_api
        api_error = getattr(hubspot.crm, HubspotObjectEnum(hs_object))
        try:
            response = basic_api.get_by_id(record_id, properties=properties)
            return response
        except api_error.ApiException as e:
            logging.error(f"An exception occured when calling {hs_object} batch_api_>update\n {e}")
    
    def get_property_names(self, object_name: str):
        """
        Function to get the property names of an object type (i.e. companies).

        Paramters:
        - object_name (string): Name of the hubspot object the properties need to come from.

        Returns
        - property_names (list): list of all the propertynames under an object
        """
        property_names = []
        try:
            api_response = self.client.crm.properties.core_api.get_all(
                object_type=object_name
            )
            api_results = api_response.to_dict()

            for result in api_results["results"]:
                property_names.append(result["name"])

            return property_names
        except properties.ApiException as e:
            logging.error(f"Exception when calling core_api->get_all: {e}\n")

    def get_all(
        self,
        hs_object: str,
        properties: list = [],
    ):
        """
        Method to retrieve all items of the given hubspot object.

        Params:
        - hs_object (str): name of the hubspot object
        - properties (list): list of properties desired to retreieve. Empty by default

        Returns:
        - list of all items items under a hubspot object.
        """
        results = []
        basic_api = getattr(self.client.crm, hs_object).basic_api
        error_api = getattr(hubspot.crm, HubspotObjectEnum(hs_object))
        try:
            api_response = basic_api.get_page(properties=properties, limit=100)
            results.extend(api_response.results)
            self.client.crm.companies.basic_api.get_page
            while api_response.paging:
                api_response = basic_api.get_page(
                    properties=properties,
                    limit=100,
                    after=api_response.paging.next.after,
                )

                results.extend(api_response.results)

        except error_api.ApiException as e:
            logging.error(e)

        logging.info(f"returning {len(results)} items of Hubspot {hs_object}.")
        return results

    def get_associations(
            self, id_: str, 
            object_type: str, 
            associated_object_type: str,
    ):
        """
        Method to retreive all associations of a specified object type.

        Params:
        - id_ (str): hubspot record id of specified object.
        - object-type (str): hubspot object type. 
        - associated_object_type (str): hubspot object of the associations to be retrieved.

        Returns:
        - list of all associations as association objects.
        """
        error_api = associations.ApiException
        basic_api = self.client.crm.associations.v4.basic_api
        try:
            response = basic_api.get_page(
                object_type=object_type,
                object_id=id_,
                to_object_type=associated_object_type,
                limit=100,
            )
            results = response.results

            while response.paging:
                response = basic_api.get_page(
                    object_type=object_type,
                    object_id=id_,
                    to_object_type=associated_object_type,
                    limit=100,
                    after=response.paging.next.after,
                )
                results.extend(response.results)
            
            return results

        except error_api as e:
            logging.error(f"Exception when calling basic_api->create: {e}\n")
    
    def filtered_search(
            self, 
            hs_object: str, 
            filters: list = [], 
            properties: list = [] , 
            associations: list = [],
    ) -> list:
        """
        Method to find one or more objects based on provided filters.
        If a value doesn't fit in the property (i.e. text in number property), an error will be thrown.

        Params:
        - hs_object (str): name of type of object to search in.
        - filters (list): applied filters searcg
        - properties (list): list of properties to be included in the return
        - associations (list): list of association object desired to be retreived.

        Returns:
        - list of object fitting search query.
        """
        search_request = {
            "limit": 100,
            "properties": properties,
            "associations": associations,
            "filterGroups": [{
                "filters": filters
            }]
        }
        simple_input_class = get_search_input_class(hs_object)
        if not simple_input_class:
            return
        
        error_api = getattr(hubspot.crm, HubspotObjectEnum(hs_object))
        search_api = getattr(self.client.crm, hs_object).search_api
        try:
            simple_input_class(search_request)
            response = search_api.do_search(public_object_search_request=search_request)
            results = response.results
            while response.paging:
                search_request["after"] = response.paging.next.after
                response = search_api.do_search(
                    public_object_search_request=search_request
                )

                results.extend(response.results)
                sleep(0.1) # too fast results in error
            
            if results:
                logging.info(f"{len(results)} items found.")
                return results
        
        except error_api.ApiException as e:
            logging.error(f"An error occured while doing a filtered search: {e}")


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
                    self.add_association(association, key, to_keep, "tickets")

        return to_keep, to_remove

    def add_association(
            self, 
            association, 
            association_type: str, 
            object_, 
            object_type: str, 
            hubspot_defined: bool = True, 
    ):
        """
        Method to add association of an object to a new object using existing connection type.

        Params:
        - association: object that will be associated.
        - association_type (str): type of association using singular form (company instead of companies).
        - object_: object that the association will be associated to.
        - object_type (str): type of association using singular form (company instead of companies).
        - hubspot_defined (str): 
        """
        association_type_id = getattr(AssociationType, association.type.upper())
        association_id = association.id
        object_id= object_.id
        defined = "HUBSPOT_DEFINED" if hubspot_defined is True else "USER_DEFINED"

        try:
            return self.client.crm.associations.v4.basic_api.create(
                object_type=object_type,
                object_id=object_id,
                to_object_type=association_type,
                to_object_id= association_id,
                association_spec=[{
                    "associationCategory": defined,
                    "associationTypeId": association_type_id,
                }],
            )
        except associations.ApiException as e:
            logging.error(f"Exception when calling batch_api->create: {e}")

    def create_association(
            self, 
            from_object_id: str, 
            from_object_type: str, 
            to_object_id: str, 
            to_object_type: str , 
            association_type: str, 
            hubspot_defined: bool = True,
    ):
        """
        Method that creates a new association.

        Params:
        - from_object_id (str): id of the from object.
        - from_object_type (str): hubspot type of the from object.
        - to_object_id (str): id of the to object.
        - to_object_type (str): hubspot type of the to object.
        - association_type (str): written out association. ex: "company_to_ticket"
        - hubspot_defined (bool): whether object is Hubspot defined or user made. Default True.

        Returns:
        - New association on succes.
        """
        association_type_id = getattr(AssociationType, association_type.upper())
        association_spec = [{
            "associationCategory": ("HUBSPOT_DEFINED" if hubspot_defined is True else "USER_DEFINED"),
            "associationTypeId": association_type_id,
        }]
        try:
            response = self.client.crm.associations.v4.basic_api.create(
                object_type= from_object_type,
                object_id=from_object_id,
                to_object_type= to_object_type,
                to_object_id= to_object_id,
                association_spec= association_spec,
            )
            return response
        except associations.ApiException as e:
            logging.error(f"Exception when calling basic_api->create: {e}")
            return

    def remove_association(
        self, 
        from_object_id: str, 
        from_object_type: str, 
        to_object_id: str, 
        to_object_type: str,
    ):
        """
        Function to remove singular association from an object.

        Params:
        - from_object_id (str): id of the from object.
        - from_object_type (str): hubspot type of the from object.
        - to_object_id (str): id of the to object.
        - to_object_type (str): hubspot type of the to object.
        """
        try:
            self.client.crm.associations.v4.basic_api.archive(
                object_type=from_object_type,
                object_id=from_object_id,
                to_object_type=to_object_type,
                to_object_id=to_object_id,
            )
            return 1 # return something when success
        except associations.ApiException as e:
            logging.error(f"Exception while trying to archive an association: {e}")
            return # None when fail

    def archive_object(self, object_id: str, hs_object: str):
        """
        Funtion to archive one object.

        Parameters:
            object_id (str): id of the object deleted.
            object_type (str): husbpot object type,
        """
        logging.info(f"Archiving {hs_object} object with record_id {object_id}.")
        basic_api = getattr(self.client.crm, HubspotObjectEnum(hs_object)).basic_api
        error_api = getattr(hubspot.crm, HubspotObjectEnum(hs_object))

        try:
            basic_api.archive(object_id)
        except error_api.ApiException as e:
            logging.error(f"Exception when calling basic_api->archive: {e}")

    def batch_archive(self, object_ids: list, hs_object:str):
        """
        Funtion to archive multiple objects at once.

        Parameters:
            object_ids (list): list of of dicts containing {"id": string} to be deleted.
            object_type (str): husbpot object type,
        """
        logging.info(f"Archiving {len(object_ids)} items from {hs_object}.")
        input_batch_class = get_batch_input_class(hs_object)
        input_batch = input_batch_class(object_ids)

        batch_api = getattr(self.client.crm, HubspotObjectEnum(hs_object)).batch_api
        error_api = getattr(hubspot.crm, HubspotObjectEnum(hs_object))

        try:
            return batch_api.archive(input_batch)
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
    - hs_object (str): name of the object type.

    Returns:
    - batch input object
    """
    batch_input_class = batch_input_map.get(HubspotObjectEnum(hs_object))
    if not batch_input_class:
        logging.error(f"Invalid hs_object: {hs_object}")
    return batch_input_class

def get_search_input_class(hs_object:str):
    """
    Method to check if object exists in batch_input_map.
    Logs as error if no input class was found.

    Params:
    - hs_object (str): name of the object type.

    Returns:
    - batch input object
    """
    simple_input_class = simple_search_map.get(HubspotObjectEnum(hs_object))
    if not simple_input_class:
        logging.error(f"Invalid hs_object: {hs_object}")
    return simple_input_class

def update_properties_list(hubspot_items: list) -> list:
    """
    Function to set a collection of hubspot objects into a correct list for updating properties.

    Params:
    - hubspot_items (list): list of all the hubspot_objects that need to be updated.

    Returns:
    - list of all items ready to be sent to Hubspot.
    """
    final_list = []
    for item in hubspot_items:
        if type(item) == dict:
            properties = item["properties"]
        else:
            properties = item.properties
        change_properties = {}
        for prop in properties:
            # not changing properties set by hubspot or the create date.
            if not prop.startswith("hs_") and prop != "createdate":
                change_properties[prop] = properties[prop]
        if type(item) == dict:
            id = item["id"]
        else:
            id = item.id
        item_dict = {
            "id": id,
            "properties": change_properties,
        }
        final_list.append(item_dict)

    return final_list

def create_filter(
        property_name: str, 
        operator: str, 
        property_value: str = None, 
        higher_value: str = "", 
        property_values: list = [],
) -> dict:
    """
    Method that returns a fitler that can be used for hubspot searches.

    Params:
    - property_name (str): internal name of the property
    - operator (str): operator for the search
    - property_value (str): value the property must have. lower value if between 2 values.
    - higer_value (str): higher value when between 2 values. 
    - property_values (list): list of values for IN or NOT_IN operators.

    Returns filter dict
    """
    if operator.upper() in  [
        "LT", "LTE", "GT", "GTE", "EQ", "NEQ"
    ]:
        return {
            "propertyName": property_name,
            "operator": operator.upper(),
            "value": property_value,
        }
    elif operator.upper() in ["BETWEEN"]:
        return {
            "propertyName": property_name,
            "operator": operator.upper(),
            "highValue": higher_value,
            "value": property_value,
        }
    elif operator.upper() in [
        "IN", "NOT_IN"
    ]:
        return {
            "propertyName": property_name,
            "operator": operator.upper(),
            "value": property_values,
        }
    elif operator.upper() in ["HAS_PROPERTY", "NOT_HAS_PROPERTY"]:
        return {
            "propertyName": property_name,
            "operator": operator.upper(),
        }
    # elif operator.upper() in [
    #     "CONTAINS_TOKEN", "NOT_CONTAINS_TOKEN"
    # ]:
    #     pass # something special ?
    else:
        logging.error("the operator doesn't exist")