import hubspot

from hubspot.crm import companies, contacts, deals, properties

"""
Module that takes care of HubSpot connection and API calls to HubSpot

global parameters:
- batch_input_map (map) map of the batch_inputs referring to the different classes designed for the different HubSpot classes
"""

batch_input_map = {
    "companies": companies.BatchInputSimplePublicObjectBatchInput,
    "contacts": contacts.BatchInputSimplePublicObjectBatchInput,
    "deals": deals.BatchInputSimplePublicObjectBatchInput,
}


class Hubspot:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.client = hubspot.Client.create(access_token=access_token)
        """
        this method returns all information of contact properties
        """

    def send_patch(self, properties: list, hs_object: str):
        """
        Function that .

        Parameters:
        - properties: data to be send
        - hs_object (string): hubspot object data will be sent to
        """
        logging.info("sending %s batch patch to hubspot" % hs_object)

        batch_input_class = batch_input_map.get(hs_object)
        if not batch_input_class:
            logging.error("Invalid hs_object: %s" % hs_object)

        batch_input = batch_input_class(inputs=properties)
        try:
            batch_api = getattr(self.client.crm, hs_object).batch_api
            response = batch_api.update(
                batch_input_simple_public_object_batch_input=batch_input
            )
        except Exception as e:
            logging.error("Exception when calling batch_api->update: %s\n" % e)

        try:
            response.errors
            errors = response.errors
            if len(errors) > 0:
                log_errors(errors)

        except Exception:
            pass

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
        except properties.ApiException as e:
            logging.error("Exception when calling core_api->get_all: %s\n" % e)


def log_errors(errors):
    """
    Function for the logging of errors in the response of the API request.

    Parameters:
    - errors: errors obtained from response

    """
    for error in errors:
        category = error.category
        context_ids = ", ".join(error.context["ids"])
        """
        The process was stopped
        """
        logging.error(
            "The process was stopped prematurely resulting from an error of category %s with record_ids: %s "
            % (category, context_ids)
        )
