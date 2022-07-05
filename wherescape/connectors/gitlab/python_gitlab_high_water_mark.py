from datetime import datetime
import logging

from ... import WhereScape


def gitlab_next_high_water_mark():
    """
    At the beginning of the load data job, this function will set the next
    high water mark. As the high water mark date value is shared over all
    Gitlab objects, this needs to be determined before the first object is
    being loaded. This date value will be stored seperately until the job
    has finished.
    """
    wherescape_instance = WhereScape()
    next_high_water_mark = datetime.today().isoformat(timespec="seconds")
    logging.info(f"Next high water mark will be {next_high_water_mark}")
    wherescape_instance.main_message = (
        f"Next high water mark will be {next_high_water_mark}"
    )
    wherescape_instance.write_parameter(
        "gitlab_high_water_mark_next", next_high_water_mark
    )


def gitlab_update_high_water_mark():
    """
    After all objects have been successfull loaded, the high water mark is
    swapped with the date set in the next high water mark parameter at the
    beginning of the job.
    """
    wherescape_instance = WhereScape()

    next_high_water_mark = wherescape_instance.read_parameter(
        "gitlab_high_water_mark_next"
    )
    wherescape_instance.write_parameter("gitlab_high_water_mark", next_high_water_mark)
    wherescape_instance.main_message = (
        f"High water mark is set to {next_high_water_mark}"
    )
    logging.info(f"High water mark is set to {next_high_water_mark}")
