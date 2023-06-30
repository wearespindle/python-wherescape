try:
    import logging
    from google_sheets import create_metadata
    # from wherescape_error_handling import get_stack_trace_str


    create_metadata()
except Exception as e:
    logging.error("Unexpected Error")