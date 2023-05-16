import logging

from hubspot.crm.contacts import (
    SimplePublicObjectInput,
    BatchInputSimplePublicObjectBatchInput,
    ApiException,
)

"""
Module where data is send to the contacts object in Hubspot
required scopes: crm.objects.contacts.write
"""


def patch_on_id(id, properties, client):
    """
    method to send singular data for Contact object to Hubspot
    """
    logging.info("sending Contact patch to hubspot")
    object_input = SimplePublicObjectInput(properties=properties)

    try:
        client.crm.contacts.basic_api.update(
            contact_id=id, simple_public_object_input=object_input
        )
    except ApiException as e:
        logging.error("Exception when calling basic_api->update: %s\n" % e)


def patch_batch(inputs, client):
    """
    method to send batch data for Contact object to Hubspot
    """
    logging.info("sending Contact batch patch to hubspot")
    batch_input = BatchInputSimplePublicObjectBatchInput(inputs=inputs)
    try:
        client.crm.contacts.batch_api.update(
            batch_input_simple_public_object_batch_input=batch_input
        )
    except ApiException as e:
        logging.error("Exception when calling batch_api->update: %s\n" % e)
