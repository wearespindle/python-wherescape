import logging
import os

from datetime import datetime
from ... import WhereScape


def nmbrs_load_data():
    wherescape_instance = WhereScape

    # first logging
    start_time = datetime.now()
    logging.info(
        "Start time: %s for nmbrs_load_data" % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )

    access_token = wherescape_instance.read_parameter("")
    table_name = wherescape_instance.load_full_name

    # Final logging
    end_time = datetime.now()
    logging.info(
        "Time elapsed: %s seconds for gitlab_load_data"
        % (end_time - start_time).seconds
    )
