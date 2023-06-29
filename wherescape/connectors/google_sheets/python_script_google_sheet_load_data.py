try: 
  from google_sheets import google_sheet_load_data
  from wherescape_error_handling import get_stack_trace_str
 
  google_sheet_load_data()

except Exception as e:
    print(-3)
    print(get_stack_trace_str("Unexpected Error"))