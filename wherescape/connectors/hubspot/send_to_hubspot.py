import logging
from hubspot.crm import companies, contacts, deals
from hubspot.client import Client

"""
This module takes care of the API calls.

global parameters:
- batch_input_map (map) map of the batch_inputs referring to the differen classes designed for the different HubSpot classes
"""

batch_input_map = {
    "companies": companies.BatchInputSimplePublicObjectBatchInput,
    "contacts": contacts.BatchInputSimplePublicObjectBatchInput,
    "deals": deals.BatchInputSimplePublicObjectBatchInput,
}


def patch(inputs: list, client: Client, hs_object: str):
    """
    Function to send a batch of patches

    Parameters:
    - input: object items and its properties with data that will be updated
    - client: hubspot client destination
    - hs_object (string): hubspot object data needs to be send to
    """
    logging.info("sending %s batch patch to hubspot" % hs_object)

    batch_input_class = batch_input_map.get(hs_object)
    if not batch_input_class:
        print("Invalid hs_object: %s" % hs_object)
        return

    batch_input = batch_input_class(inputs=inputs)
    try:
        batch_api = getattr(client.crm, hs_object).batch_api
        batch_api.update(batch_input_simple_public_object_batch_input=batch_input)
    except Exception as e:
        print("Exception when calling batch_api->update: %s\n" % e)
