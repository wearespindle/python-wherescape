import logging
import hubspot

from hubspot.crm.properties import ApiException
from .send_data import (
    send_company_object,
    send_contact_object,
    send_deal_object,
)

# from pprint import pprint
# from hubspot.crm.companies import SimplePublicObjectInput, ApiException

"""
module to 
"""


class Hubspot:
    def __init__(self, access_token):
        self.access_token = access_token
        self.client = hubspot.Client.create(access_token=access_token)

    def get_company_properties(self):
        """
        this method returns all information of company properties
        """
        logging.info("getting properties of companies")
        return self.get_object_properties("companies")

    def get_deal_properties(self):
        """
        this method returns all information of deal properties
        """
        logging.info("getting properties of deals")
        return self.get_object_properties("deals")

    def get_contact_properties(self):
        """
        this method returns all information of contact properties
        """
        logging.info("getting properties of contacts")
        return self.get_object_properties("contacts")

    def send_company_patch(self, **args):
        """
        method for sending patches for companies
        """
        inputs = args["inputs"]

        if len(inputs) == 1:
            id = inputs[0]["id"]
            properties = inputs[0]["properties"]
            send_company_object.patch_company_on_id(id, properties, self.client)
        elif len(inputs) > 1:
            send_company_object.patch_company_batch(inputs, self.client)

    def send_contact_patch(self, **args):
        """
        method for sending patches for contacts
        """
        inputs = args["inputs"]

        if len(inputs) == 1:
            id = inputs[0]["id"]
            properties = inputs[0]["properties"]
            send_contact_object.patch_company_on_id(id, properties, self.client)
        elif len(inputs) > 1:
            send_contact_object.patch_company_batch(inputs, self.client)

    def send_deal_patch(self, **args):
        """
        method for sending patches for deals
        """
        inputs = args["inputs"]

        if len(inputs) == 1:
            id = inputs[0]["id"]
            properties = inputs[0]["properties"]
            send_deal_object.patch_company_on_id(id, properties, self.client)
        elif len(inputs) > 1:
            send_deal_object.patch_company_batch(inputs, self.client)

    def get_object_properties(self, object_name: str):
        """
        This method returns a list of all the properties that fall under an object
        """
        property_names = []
        try:
            api_response = self.client.crm.properties.core_api.get_all(
                object_type="companies", archived=False
            )
            api_results = api_response.to_dict()

            for result in api_results["results"]:
                property_names.append(result["name"])

            return property_names
        except ApiException as e:
            logging.error("Exception when calling core_api->get_all: %s\n" % e)
