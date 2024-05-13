import logging
from datetime import datetime

from wherescape.connectors.hubspot.hubspot_wrapper import Hubspot
from wherescape.wherescape import WhereScape

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
        logging.error(f"There was no parameter found with the name {parameter_name}.")

    hubspot = Hubspot(access_token)
    
    all_tickets = hubspot.get_all("tickets", ticket_properties)

    double_ticket_ids = get_double_nerd_ids(all_tickets)
    delete_tickets = []
    update_tickets = []
    # Work through all nerd ticket ids.
    logging.info("Merging tickets locally.")
    for id_ in double_ticket_ids:
        ticket_list = get_double_tickets(all_tickets, str(id_))

        # merge the tickets until only one is left.
        count = len(ticket_list)
        while count > 1:
            keep, delete = hubspot.merge_tickets(ticket_list[0], ticket_list[1])
            ticket_list[0] = keep
            ticket_list.pop(1)
            delete_tickets.append(delete)
            count = len(ticket_list)
        update_tickets.append(ticket_list[0])

    logging.info(f"Updating {len(update_tickets)} tickets.")
    hubspot.update_batch(update_tickets, "tickets")

    delete_ids = []
    for ticket in delete_tickets:
        delete_ids.append({"id": ticket.id})

    logging.info(f"Archiving {len(delete_ids)} duplicate tickets")
    print(f"Archiving {len(delete_ids)} duplicate tickets")
    # Does max 100 at a time
    if len(delete_ids) > 100:
        while len(delete_ids) > 100:
            batch_list = delete_ids[:100]
            hubspot.batch_archive(batch_list, "tickets")

            del delete_ids[:100]
    # delete remaining < 100 items
    hubspot.batch_archive(delete_ids, "tickets")


def get_double_nerd_ids(tickets: list) -> list:
    """
    Function to retrieve all nerd ticket id that appear multiple times.

    Params:
    - tickets (list): list of hubspot tickets

    Returns:
    - list if nerd ticket id's that appear more than once
    """
    seen_nerd_ticket_id = {}
    for ticket in tickets:
        nerds_ticket = ticket.properties["nerds_ticket_id"]
        if ticket.properties["nerds_ticket_id"] is not None:

            if nerds_ticket not in seen_nerd_ticket_id:
                seen_nerd_ticket_id[nerds_ticket] = 1
            else:
                seen_nerd_ticket_id[nerds_ticket] += 1

    return [k for k, count in seen_nerd_ticket_id.items() if count > 1]

def get_double_tickets(tickets: list, id_: str) -> list:
    """
    Function to collect all tickets with the provided id.

    Params:
    - tickets (list): list of Hubspot tickets
    - id_ (str): nerds ticket id to look for.

    Returns:
    - list of tickets with the given nerds_ticket_id
    """
    return [ticket for ticket in tickets if (ticket.properties["nerds_ticket_id"]) == id_]
