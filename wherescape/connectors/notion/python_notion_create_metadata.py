from datetime import datetime
from notion_wrapper import Notion
import logging

from wherescape import WhereScape
from wherescape.helper_functions import prepare_metadata_query, create_column_names


def notion_create_metadata():
    wherescape = WhereScape()
    start_time = datetime.now()
    logging.info(
        "Start time: %s for notion_load_data" % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )

    # Initialise WhereScape
    key = wherescape.read_parameter("notion_key")
    wherescape_object_id = wherescape.object_key
    notion_database_id = wherescape.top_level_name

    # Get the Notion database (definition)
    notion = Notion(key)
    title, database = notion.get_notion_database(notion_database_id)
    display_names, types = notion.get_notion_database_columns(database)
    logging.info(f'Fetched database definition "{title}" from Notion')

    # Prepare the sql
    columns = create_column_names(display_names)
    sql = prepare_metadata_query(
        wherescape_object_id,
        "Notion database - " + title,
        columns=columns,
        display_names=display_names,
        types=types,
    )

    # Execute the sql
    wherescape.push_to_meta(sql)
    if len(wherescape.messages) > 0:
        for message in wherescape.messages:
            logging.info(message)
    if len(wherescape.error_messages) > 0:
        for error in wherescape.error_messages:
            logging.error(error)
    else:
        logging.info(f"Created metadata table for {title} created.")

    # Final logging
    end_time = datetime.now()
    logging.info(
        "Time elapsed: %s seconds for notion_load_data"
        % (end_time - start_time).seconds
    )
