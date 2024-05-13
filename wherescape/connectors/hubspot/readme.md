# HubSpot connector

HubSpot connector for WhereScape. takes care of updating HubSpot data.

# Preparation


## HubSpot app
Create a private app with the required scopes. These scopes will depend on your needs. 
The following are needed for all functionalities currently available

crm.objects.contacts.read
crm.objects.contacts.write
crm.objects.companies.write
crm.objects.companies.read
crm.objects.deals.read
crm.objects.deals.write
crm.export
tickets

## WhereScape Parameters
Add the following parameter to WhereScape

* `hubspot_access_token`

Fill in the correct access token referring to the private app created in HubSpot.

## load or stage table
Create a load table for each HubSpot object where data will be sent to. 
The table name needs to contain the following information:
* hubspot object name (singlular)
* type of api_call

if environment needs to be specified, add this as well to the table name.

The table also requires

## Host script
Create a new python host script and add it to the load table. Example code:

```
from wherescape.connectors.hubspot.collect_data import hubspot_load_data

hubspot_load_data()
```

## multiple HubSpot environments
If there are multiple environments, this script will be able to determine the desired environment
using the names of the acccess token. This name consists of a required base name and an optional word specifying to the environment.
The base name is `hubspot_access_token`. To specify which Hubspot environment, simply add `_{specific}` to the token name, where the word
specific can be replaced with the word specifying the environment.
This word can by any word, as long as it is also mentioned in the table name. 
for example, if the token name for an environment is `voys`, the table name could be `voys_stage_send_data` and the access token would be `hubspot_access_token_voys`.
If only one environment exists, there's no need to specify the environment as the script will use the base name.

For a connection with the Sandbox, add `_dev` at the end of the parameter.

## Merging Tickets
The method merge_double_tickets in connectors.hubspot.ticket_merge.py provides functionality to merge ticket information 
based on having the same nerds_ticket_id while keeping content and note associations of all the merged tickets. 
One of each ticket will be kept and all others are archieved. The property `nerds_ticket_id` is used to find these 
double tickets and is therefore a required property.

### Usage
To use this script, run the method with the name of the wherescape parameter referring to the required token for the Hubspot connection.

This method does not need or provide any input or output related to Wherescape and thus doesn't require to be connected to any table. However, because a Wherescape is still initialised, it will still have to be run from the WhereScape environment.

# Usage
After creating the table, attach the host script to the table. 

If multiple environments are used, make sure the name used to refer to the environment is 
the same in both table name and the parameter `hubspot_access_token`.