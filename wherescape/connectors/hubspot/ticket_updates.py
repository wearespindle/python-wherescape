import logging
from datetime import datetime

from wherescape.connectors.hubspot.hubspot_wrapper import Hubspot, create_filter
from wherescape.connectors.hubspot.utils import get_double_nerd_ids, get_double_tickets
from wherescape.wherescape import WhereScape

"""
This module requires only the access token to update information
"""

# Ticket properties we need or want to make sure we have the info in the final ticket.
ticket_properties = [
    "content",
    "hubspot_owner_id",
    "nerds_customer_id",
    "nerds_ticket_id",
    "nerds_agent",
    "notes",
]

def merge_double_tickets(parameter_name: str):
    """
    Function start the process of merging tickets with the same nerds ticket id.

    Params:
    - access_token (str): token for connection to HubSpot
    """
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info("connecting to Wherescape")
    wherescape_instance = WhereScape()
    logging.info(
        f"Start time: {start_time} for hubspot merge_double_tickets"
    )
    access_token = wherescape_instance.read_parameter(parameter_name)
    if access_token is None:
        logging.error(f"Nothing Token found under parameter: {parameter_name}.")
        exit()        

    hubspot = Hubspot(access_token)
    
    all_tickets = hubspot.get_all("tickets", ticket_properties)

    double_ticket_ids = get_double_nerd_ids(all_tickets)
    delete_tickets = []
    update_tickets = []
    # Work through all nerd ticket ids.
    logging.info("Merging tickets locally.")
    for id_ in double_ticket_ids:
        ticket_list = get_double_tickets(all_tickets, str(id_))

        # Merge the tickets.
        # Set first ticket to keep and remove from list.
        keep_ticket = ticket_list.pop(0)
        count = len(ticket_list)
        while count > 0:
            # Merge current keep and first in ticket_list and get (new) keep ticket
            keep_ticket, delete = hubspot.merge_tickets(keep_ticket, ticket_list.pop(0))
            delete_tickets.append(delete)
            count = len(ticket_list)
        update_tickets.append(keep_ticket)

    logging.info(f"Updating {len(update_tickets)} tickets.")
    # Does max 100 at a time
    if len(update_tickets) > 100:
        while len(update_tickets) > 100:
            batch_list = update_tickets[:100]
            hubspot.batch_archive(batch_list, "tickets")

            del update_tickets[:100]
    result = hubspot.update_batch(update_tickets, "tickets") 
    if result is None:
        exit() # exit if nothing was updated. to avoid archiving everything

    delete_ids = []
    for ticket in delete_tickets:
        delete_ids.append({"id": ticket.id})

    logging.info(f"Archiving {len(delete_ids)} duplicate tickets")
    # Does max 100 at a time
    if len(delete_ids) > 100:
        while len(delete_ids) > 100:
            batch_list = delete_ids[:100]
            hubspot.batch_archive(batch_list, "tickets")

            del delete_ids[:100]
    # delete remaining < 100 items
    hubspot.batch_archive(delete_ids, "tickets")

def hubspot_update_company_associaton(parameter_name: str):
    """
    Function to set the right company to a hubspot ticket based on ticket propery nerds_customer_id and company property client_number.

    Parameter:
    - parameter_name (str): name of the parameter containing the correct access token
    """
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info("connecting to Wherescape")
    wherescape_instance = WhereScape()
    logging.info(
        f"Start time: {start_time} for hubspot merge_double_tickets"
    )
    # get parameter
    access_token = wherescape_instance.read_parameter(parameter_name)
    if access_token is None:
        logging.error(f"Nothing Token found under parameter: {parameter_name}.")
        exit()

    hubspot = Hubspot(access_token)
    
    # Get Nerds company.
    filters = []
    filters.append(create_filter("domain", "EQ", "nerds.nl"))
    filters.append(create_filter("city", "EQ", "utrecht"))
    try:
        nerds_company = hubspot.filtered_search("companies", filters, ["associations.company"])[0]
    except: # if something goes wrong when getting Nerds company
        logging.error("problem while locating company")
    

    # Get tickets with Nerds as associated company.
    ticket_filters = []
    ticket_filters.append(create_filter("associations.company", "EQ", nerds_company.id))
    ticket_filters.append(create_filter("nerds_customer_id", "HAS_PROPERTY"))
    tickets = hubspot.filtered_search(hs_object="tickets", filters=ticket_filters, properties=["nerds_customer_id"])
    logging.info("%i tickets found with " % len(tickets) )

    for ticket in tickets:
        correct_company = None
        customer_id = ticket.properties["nerds_customer_id"]

        # Remove nerds ticket association
        hubspot.remove_association(nerds_company.id, "companies", ticket.id, "tickets")

        # Get associated Companies
        associated_companies = hubspot.get_associations(ticket.id, "ticket", "company")
        # see if correct company is already there.
        for association in associated_companies:
            company = hubspot.get_object(
                association.to_object_id, "companies", ["client_id", "domain", "city"]
            )
            if company is not None: # we can stop once we find the correct one
                correct_company = company
                break
        
        if correct_company is None:
            filters = []
            # Client id is numeric, so if customer id is not, the search would give an error.
            if customer_id.isnumeric():
                filters.append(create_filter("client_id", "EQ", customer_id))
                association = hubspot.filtered_search("companies", filters)

                if association is not None and len(association) == 1:
                    response = hubspot.create_association(
                        from_object_id= company.id, 
                        from_object_type= "companies",
                        to_object_id= ticket.id, 
                        to_object_type="tickets",
                        association_type="primary_company_to_ticket"
                    )
                    if response is None: # no response indicates it failed
                        logging.warning("Correct company could not be set for ticket with record id %s." % ticket.id)
                    
            else:
                logging.warning(f"customer_id could not be used: {customer_id}")
        

