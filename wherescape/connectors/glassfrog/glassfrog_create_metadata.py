"""
Create the WhereScape load-table metadata for Glassfrog meetings.
"""

import logging
from datetime import datetime

from ...helper_functions import create_column_names, create_display_names, prepare_metadata_query
from ...wherescape import WhereScape


# Source of truth for the load-table shape. Keep in sync with _build_row() in glassfrog_load_data.py.
# dss_record_source / dss_load_date are appended by prepare_metadata_query() — do not list them here.
#
# Each column carries a "comment" that lands in the warehouse column metadata. Where the column
# may contain PII, the comment is prefixed with a sensitivity label per the company GDPR policy
# (see CLAUDE.md → "Sensitive data labelling"). For Glassfrog the foreign keys into the user
# table (facilitator_id, secretary_id) are GDPR_MEDIUM because they identify specific persons.
COLUMNS: dict[str, dict[str, str]] = {
    "id": {
        "type": "bigint",
        "comment": (
            "[GDPR_LOW] Glassfrog meeting id (primary key of the load table); "
            "combined with the app URL it can re-identify attendees via the Glassfrog UI."
        ),
    },
    "circle_id": {
        "type": "bigint",
        "comment": "Foreign key to the Glassfrog circle this meeting belongs to.",
    },
    "circle_name": {
        "type": "text",
        "comment": "Name of the circle this meeting belongs to (resolved via /api/v3/circles).",
    },
    "meeting_type": {
        "type": "text",
        "comment": "Meeting kind: governance or tactical — indicates which endpoint the row came from.",
    },
    "started_at": {
        "type": "timestamp",
        "comment": "Meeting start timestamp; falls back to created_at when started_at is absent.",
    },
    "ended_at": {
        "type": "timestamp",
        "comment": "Meeting end timestamp; nullable while a meeting is still in progress.",
    },
    "occurred_on": {
        "type": "date",
        "comment": "Date prefix of started_at — convenient for period grouping in stage / fact tables.",
    },
    "facilitator_id": {
        "type": "bigint",
        "comment": "[GDPR_MEDIUM] Glassfrog user id of the meeting facilitator (foreign key into a PII table).",
    },
    "secretary_id": {
        "type": "bigint",
        "comment": "[GDPR_MEDIUM] Glassfrog user id of the meeting secretary (foreign key into a PII table).",
    },
    "attendee_count": {
        "type": "int",
        "comment": "Number of attendees on the meeting (length of links.attendees).",
    },
    "url": {
        "type": "text",
        "comment": (
            "[GDPR_LOW] Glassfrog app URL pointing to this meeting; "
            "navigating to it in the UI reveals attendees, so treat as a re-identification vector."
        ),
    },
}


def glassfrog_create_metadata():
    """Create / refresh metadata rows for the Glassfrog meetings load table.

    DO NOT schedule this function — it drops the target load table on every run
    (see DROP TABLE below). Run it manually whenever the column set in COLUMNS
    changes; RED's load template will re-create the physical table from the
    refreshed metadata on the next load run.

    Idempotent: dropping the table first means a re-run after column-set changes
    leaves the warehouse in a clean state.
    """
    start_time = datetime.now()
    ws = WhereScape()
    logging.info(f"Start time: {start_time:%Y-%m-%d %H:%M:%S} for glassfrog_create_metadata")

    table_name = ws.load_full_name
    logging.info(f"Dropping target table {table_name} if it exists (idempotent re-run)")
    ws.push_to_target(f"DROP TABLE IF EXISTS {table_name}")

    source_columns = list(COLUMNS.keys())
    column_names = create_column_names(source_columns)
    display_names = create_display_names(source_columns)
    types = [meta["type"] for meta in COLUMNS.values()]
    comments = [meta["comment"] for meta in COLUMNS.values()]

    sql = prepare_metadata_query(
        ws.object_key,
        "Glassfrog API - meetings",
        columns=column_names,
        display_names=display_names,
        types=types,
        comments=comments,
        source_columns=source_columns,
    )
    ws.push_to_meta(sql)
    ws.main_message = f"Created {len(column_names)} columns for Glassfrog meetings"
    logging.info(ws.main_message)
    logging.info(f"Time elapsed: {(datetime.now() - start_time).seconds}s for glassfrog_create_metadata")


if __name__ == "__main__":
    # __main__ is executed when running the module standalone

    # set up the environment
    #   NB. ws_env.py can be created based on ../../ws_env_template.py
    #       and needs to live in the same directory as this file
    from ws_env import setup_env

    setup_env("load_gf_meetings", schema="load")

    # call the main function
    glassfrog_create_metadata()
