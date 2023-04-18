import hubspot
import logging
import wherescape

from hubspot.crm.companies import (
    SimplePublicObjectInput,
    BatchInputSimplePublicObjectBatchInput,
    ApiException,
)

"""
Module that sends data to Hubspot
"""


def patch_company_on_id(id, properties, client):
    object_input = SimplePublicObjectInput(properties=properties)
    try:
        api_response = client.crm.companies.basic_api.update(
            company_id=id, simple_public_object_input=object_input
        )
        logging.info("sending companypatch to hubspot")
        logging.debug(api_response)
        print(api_response)
    except ApiException as e:
        print("Exception when calling basic_api->update: %s\n" % e)


def patch_company_batch(inputs, client):
    batch_input = BatchInputSimplePublicObjectBatchInput(inputs=inputs)
    try:
        api_response = client.crm.companies.batch_api.update(
            batch_input_simple_public_object_batch_input=batch_input
        )
        logging.info("sending companypatch to hubspot")
        logging.debug(api_response)
        print(api_response)
    except ApiException as e:
        print("Exception when calling batch_api->update: %s\n" % e)