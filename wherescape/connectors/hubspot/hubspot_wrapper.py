import logging
import hubspot
# import send_data

from hubspot.crm.companies import SimplePublicObjectInput, ApiException
"""
module to 
"""



class Hubspot:
    def __init__(self, access_token):
        self.access_token = access_token
        self.client = hubspot.Client.create(access_token= access_token)

    # # send updates to the multiple Companies in Hubspot
    # def send_batch_company_patch(self, int: id, properties):
    #     pass

    # send updates to one Company
    def send_company_patch(self, **args):
        properties = args['properties']
        
        if 'id' in args:
            id = args['company_id']
        
        # print(properties)
        simple_public_object_input = SimplePublicObjectInput(properties=properties)
        try:
            api_response = self.client.crm.companies.basic_api.update(company_id=id, simple_public_object_input=simple_public_object_input)
            # pprint(api_response)
        except ApiException as e:
            print("Exception when calling basic_api->update: %s\n" % e)

    
        
