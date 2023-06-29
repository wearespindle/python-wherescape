try:
    from google_sheets import create_metadata
    from wherescape_error_handling import get_stack_trace_str


    create_metadata()
except Exception as e:
    print(-3)
    print(get_stack_trace_str("Unexpected Error"))