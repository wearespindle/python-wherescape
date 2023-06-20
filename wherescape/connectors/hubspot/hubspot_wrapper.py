import logging
import hubspot

from hubspot.crm.properties import ApiException
from . import send_to_hubspot

"""
module that makes sure the data is prepared correctly depending on where and how it will be send.
Also provides the properties depending on the object.
"""


class Hubspot:
    def __init__(self, access_token):
        self.access_token = access_token
        self.client = hubspot.Client.create(access_token=access_token)
        """
        this method returns all information of contact properties
        """

    def send_patch(self, properties: list, hs_object: str):
        """
        Function that sends to patch.

        Parameters:
        - properties: data to be send
        - hs_object (string): hubspot object data will be send to
        """
        send_to_hubspot.patch(properties, self.client, hs_object)

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
        except ApiException as e:
            logging.error("Exception when calling core_api->get_all: %s\n" % e)
