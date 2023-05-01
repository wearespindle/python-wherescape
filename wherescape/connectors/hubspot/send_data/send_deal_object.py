import logging

from hubspot.crm.deals import (
    SimplePublicObjectInput,
    BatchInputSimplePublicObjectBatchInput,
    ApiException,
)

"""
Module where data is send to the deals object in Hubspot
required scopes: crm.objects.deals.write
"""


def patch_on_id(id, properties, client):
    """
    method to send singular data for Company object to Hubspot
    """
    object_input = SimplePublicObjectInput(properties=properties)
    try:
        api_response = client.crm.deals.basic_api.update(
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
        api_response = client.crm.deals.batch_api.update(
            batch_input_simple_public_object_batch_input=batch_input
        )
        logging.info("sending company batch patch to hubspot")
        logging.debug(api_response)
    except ApiException as e:
        logging.info("Exception when calling batch_api->update: %s\n" % e)
