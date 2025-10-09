from datetime import datetime
import logging
import os

# Add the current directory to the path to import anythingllm_wrapper
from .anythingllm_wrapper import get_all_embed_chats
from .anythingllm_create_metadata import EXPECTED_COLUMNS

# Import WhereScape from wherescape_os
from ...wherescape import WhereScape
from ...wherescape.helper_functions import create_column_names


def anythingllm_load_data_chats():
    """
    Function to be called from the host script in WhereScape. Will import
    chat data to the load table. This is the glue between the anythingllm_wrapper
    and WhereScape.
    """
    start_time = datetime.now()
    # First initialise WhereScape to setup logging
    logging.info("Connecting to WhereScape")
    wherescape_instance = WhereScape()
    logging.info(
        f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')} for anythingllm_load_data_chats"
    )

    # Get the relevant values from WhereScape
    api_key = os.getenv("WSL_SRCCFG_APIKEY")
    embed_uuid = os.getenv("WSL_SRCCFG_USER")
    base_url = os.getenv("WSL_SRCCFG_URL")
    table_name = wherescape_instance.load_full_name

    # Request data from AnythingLLM
    logging.info("Requesting data from AnythingLLM")
    values = get_all_embed_chats(embed_uuid, api_key, base_url)

    if values:
        # Get column names from the first record
        if len(values) > 0:
            columns = list(values[0].keys())
        else:
            logging.info("No data received from AnythingLLM")
            return

        # Check if received columns match expected columns
        received_set = set(columns)
        expected_set = set(EXPECTED_COLUMNS)

        if received_set != expected_set:
            missing_columns = expected_set - received_set
            extra_columns = received_set - expected_set

            warning_msg = "Column mismatch detected in AnythingLLM data!"
            if missing_columns:
                warning_msg += f" Missing columns: {sorted(missing_columns)}."
            if extra_columns:
                warning_msg += f" Unexpected columns: {sorted(extra_columns)}."

            logging.warning(warning_msg)

        # Mask sensitive fields (prompt, response_text, connection_ip) with [MASKED]
        for record in values:
            if "prompt" in record:
                record["prompt"] = "[MASKED]"
            if "response_text" in record:
                record["response_text"] = "[MASKED]"
            if "connection_ip" in record:
                record["connection_ip"] = "[MASKED]"

        # Prepare columns names for query.
        columns = create_column_names(columns)
        columns.append("dss_record_source")
        columns.append("dss_load_date")

        # Append dss column data to all rows.
        rows = []
        for record in values:
            row = list(record.values())
            row.append("AnythingLLM api - chats")
            row.append(start_time)
            rows.append(row)

        # Prepare the sql
        logging.info("Preparing insert query")
        column_names_string = ",".join(column for column in columns)
        question_mark_string = ",".join("?" for _ in columns)
        sql = f"INSERT INTO {table_name} ({column_names_string}) VALUES ({question_mark_string})"

        # Execute the sql
        wherescape_instance.push_many_to_target(sql, rows)
        logging.info(f"Successfully inserted {len(rows)} rows in to the load table.")

        # Add success message
        wherescape_instance.main_message = (
            f"Successfully inserted {len(rows)} rows in to the load table."
        )

    else:
        logging.info("No object changes received from AnythingLLM")

    # Final logging
    end_time = datetime.now()
    logging.info(
        f"Time elapsed: {(end_time - start_time).seconds} seconds for anythingllm_load_data_chats"
    )
