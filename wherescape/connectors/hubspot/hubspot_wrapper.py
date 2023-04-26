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
        property_names = []
        try:
            api_response = self.client.crm.properties.core_api.get_all(
                object_type="companies", archived=False
            )

            results = api_response.to_dict()

            for key in results.keys():
                if type(results) == dict:
                    property_names.append(results[key]["name"])

            return property_names

        except ApiException as e:
            logging.error("Exception when calling core_api->get_all: %s\n" % e)

    def get_deal_properties(self):
        return self.get_object_properties("deals")

    def get_contact_properties(self):
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
        property_names = []
        try:
            results = self.client.crm.properties.core_api.get_all(
                object_type=object_name, archived=False
            )

            logging.info(type(results))
            i = 0

            print(results.size())

            for property in results:
                property_names.append(results["property"].name)

            return property_names

        except ApiException as e:
            logging.error("Exception when calling core_api->get_all: %s\n" % e)
