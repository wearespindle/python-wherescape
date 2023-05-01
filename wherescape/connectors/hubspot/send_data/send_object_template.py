import logging

from hubspot.crm.OBJECT_NAME import (
    SimplePublicObjectInput,
    BatchInputSimplePublicObjectBatchInput,
    ApiException,
)

"""
This module is a template for the Hubspot object calling.
Replace the name OBJECT_NAME with the name of the object from hubspot (for example, deals) and it should work
"""


def patch_on_id(id, properties, client):
    """
    method to send singular data for Company object to Hubspot
    """
    object_input = SimplePublicObjectInput(properties=properties)
    try:
        api_response = client.crm.OBJECT_NAME.basic_api.update(
            company_id=id, simple_public_object_input=object_input
        )
        logging.info("sending company patch to hubspot")
        logging.debug(api_response)
    except ApiException as e:
        logging.error("Exception when calling basic_api->update: %s\n" % e)


def patch_batch(inputs, client):
    """
    method to send batch data for Company object to Hubspot
    """
    batch_input = BatchInputSimplePublicObjectBatchInput(inputs=inputs)
    try:
        api_response = client.crm.OBJECT_NAME.batch_api.update(
            batch_input_simple_public_object_batch_input=batch_input
        )
        logging.info("sending company batch patch to hubspot")
        logging.debug(api_response)
    except ApiException as e:
        logging.info("Exception when calling batch_api->update: %s\n" % e)
