from datetime import datetime
from notion_wrapper import Notion
import logging

from wherescape import WhereScape
from wherescape.helper_functions import create_column_names
from wherescape.logging import initialise_wherescape_logging


def notion_load_data():
    wherescape = WhereScape()
    start_time = datetime.now()
    logging.info(
        "Start time: %s for notion_load_data" % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )

    # Initialise WhereScape
    key = wherescape.read_parameter("notion_key")
    notion_database_id = wherescape.top_level_name
    table_name = wherescape.load_full_name

    # Get the Notion database
    notion = Notion(key)
    title, database = notion.get_notion_database(notion_database_id)
    logging.info(f'Fetched database definition "{title}" from Notion')
    display_names, types = notion.get_notion_database_columns(database)
    columns = create_column_names(display_names)
    values = notion.get_notion_database_data(database)

    # Prepare the sql
    column_names_string = ",".join(column for column in columns)
    question_mark_string = ",".join("?" for _ in columns)
    sql = f"INSERT INTO {table_name} ({column_names_string}) VALUES ({question_mark_string})"

    # Execute the sql
    wherescape.push_many_to_target(sql, values)
    if len(wherescape.messages) > 0:
        for message in wherescape.messages:
            logging.info(message)
    if len(wherescape.error_messages) > 0:
        for error in wherescape.error_messages:
            logging.error(error)
    else:
        logging.info(f"Data successfully inserted in to the load table.")

    # Final logging
    end_time = datetime.now()
    logging.info(
        "Time elapsed: %s seconds for notion_load_data"
        % (end_time - start_time).seconds
    )
