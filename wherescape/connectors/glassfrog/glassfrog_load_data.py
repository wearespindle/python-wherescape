"""
Load Glassfrog meetings (governance + tactical) into the WhereScape warehouse.
"""

import logging
from datetime import datetime

from ...helper_functions import create_column_names
from ...wherescape import WhereScape
from .glassfrog_create_metadata import COLUMNS
from .glassfrog_wrapper import Glassfrog


SOURCE = "https://api.glassfrog.com/api/v3/"


def _build_row(meeting_type: str, meeting: dict, circle_names: dict[int, str], load_ts: datetime) -> list:
    links = meeting.get("links") or {}
    started_at = meeting.get("started_at") or meeting.get("created_at")
    occurred_on = started_at[:10] if isinstance(started_at, str) and len(started_at) >= 10 else None
    attendees = links.get("attendees") or []
    circle_id = links.get("circle") or meeting.get("circle_id")
    return [
        meeting.get("id"),
        circle_id,
        circle_names.get(circle_id),
        meeting_type,
        started_at,
        meeting.get("ended_at"),
        occurred_on,
        links.get("facilitator"),
        links.get("secretary"),
        len(attendees),
        meeting.get("url"),
        SOURCE,
        load_ts,
    ]


def glassfrog_load_data():
    """Full reload of governance + tactical meetings into the load table."""
    start_time = datetime.now()
    ws = WhereScape()
    logging.info(f"Start time: {start_time:%Y-%m-%d %H:%M:%S} for glassfrog_load_data")

    api_key = ws.read_parameter("glassfrog_apikey")
    if not api_key:
        ws.main_message = "Error: WhereScape Parameter 'glassfrog_apikey' is not set"
        logging.error(ws.main_message)
        return

    table_name = ws.load_full_name
    gf = Glassfrog(api_key)

    logging.info("Fetching circles for name enrichment")
    circle_names = {c["id"]: c.get("name") for c in gf.get_circles()}

    logging.info("Fetching meetings (governance + tactical)")
    typed_meetings = gf.get_all_meetings()

    if not typed_meetings:
        ws.main_message = "No meetings returned from Glassfrog"
        ws.update_task_log(inserted=0)
        logging.info(ws.main_message)
        return

    rows = [_build_row(mt, m, circle_names, start_time) for (mt, m) in typed_meetings]

    columns = create_column_names(list(COLUMNS.keys())) + ["dss_record_source", "dss_load_date"]
    cols_sql = ",".join(columns)
    qmarks = ",".join("?" for _ in columns)
    sql = f"INSERT INTO {table_name} ({cols_sql}) VALUES ({qmarks})"

    ws.push_many_to_target(sql, rows)
    ws.main_message = f"Loaded {len(rows)} Glassfrog meetings into {table_name}"
    ws.update_task_log(inserted=len(rows))
    logging.info(ws.main_message)
    logging.info(f"Time elapsed: {(datetime.now() - start_time).seconds}s for glassfrog_load_data")


if __name__ == "__main__":
    # __main__ is executed when running the module standalone

    # set up the environment
    #   NB. ws_env.py can be created based on ../../ws_env_template.py
    #       and needs to live in the same directory as this file
    from ws_env import setup_env

    setup_env("load_gf_meetings", schema="load")

    # call the main function
    glassfrog_load_data()
