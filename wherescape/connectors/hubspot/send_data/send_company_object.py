import logging

from hubspot.crm.companies import (
    SimplePublicObjectInput,
    BatchInputSimplePublicObjectBatchInput,
    ApiException,
)


def patch_company_on_id(id, properties, client):
    """
    method to send singular data for Company object to Hubspot
    """
    object_input = SimplePublicObjectInput(properties=properties)
    try:
        api_response = client.crm.companies.basic_api.update(
            company_id=id, simple_public_object_input=object_input
        )
        logging.info("sending company patch to hubspot")
        logging.debug(api_response)
    except ApiException as e:
        logging.error("Exception when calling basic_api->update: %s\n" % e)


def patch_company_batch(inputs, client):
    """
    method to send batch data for Company object to Hubspot
    """
    batch_input = BatchInputSimplePublicObjectBatchInput(inputs=inputs)
    try:
        api_response = client.crm.companies.batch_api.update(
            batch_input_simple_public_object_batch_input=batch_input
        )
        logging.info("sending company batch patch to hubspot")
        logging.debug(api_response)
    except ApiException as e:
        logging.info("Exception when calling batch_api->update: %s\n" % e)
