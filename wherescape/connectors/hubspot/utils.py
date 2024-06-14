
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
