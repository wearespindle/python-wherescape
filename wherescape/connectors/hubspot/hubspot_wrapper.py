# import logging
import hubspot

# import send_data
from . import send_data

# from pprint import pprint
# from hubspot.crm.companies import SimplePublicObjectInput, ApiException

"""
module to 
"""


class Hubspot:
    def __init__(self, access_token):
        self.access_token = access_token
        self.client = hubspot.Client.create(access_token=access_token)

    def send_company_patch(self, **args):
        inputs = args["inputs"]
        if len(inputs) == 1:
            id = inputs[0]["id"]
            properties = inputs[0]["properties"]
            send_data.patch_company_on_id(id, properties, self.client)
        elif len(inputs) > 1:
            send_data.patch_company_batch(inputs, self.client)
