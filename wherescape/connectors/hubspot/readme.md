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

## WhereScape Parameters
Add the following parameter to WhereScape

* `hubspot_access_token`

Fill in the correct access token referring to the private app created in HubSpot
For a connection with the Sandbox, add `_sandbox` to the above mentioned parameter.

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
If there are multiple environments, this script will distinguish which 
To distinguish between multiple environments with different access tokens, a one word 
name referring to the environment should be added at the end of the parameter `hubspot_access_token` and the table name.
for example: if the environmentname would be "voys", the parameter would be called `hubspot_access_token_voys`

# Usage
After creating the table, attach the host script to the table. 

If multiple environments are used, make sure the name used to refer to the environment is 
the same in both table name and the parameter `hubspot_access_token`