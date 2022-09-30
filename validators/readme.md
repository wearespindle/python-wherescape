# Validators

Validators are functions that can be called to validate target consistency

These tests have been included for star models:

- fact_dimension_join : counts the number of dimension keys in all facts that have the value 0, and also logs fact_dimension_size : counts the number of records in all facts and dimension

To run this test from Wherescape, you need to create a dummy (load) table and a host script. The host script imports the check_fact_dimension_join from a directory that has to be in your pythonpath.  

```python
-- host script `python_create_csv_fact_dimension_check` 
from check_fact_dimension_join import check_fact_dimension_join

check_fact_dimension_join(output_file_location=r"C:\Temp")
```

When running this host script from the scheduler as, the dummy load table is takes care of setting the target environment variables.

## Future

These tests have been included for datastores:

- datastore_dss_current_flag_P : counts the number of records with current_flag = P (future)
- dim_hist_dss_current_flag_P : counts the number of records with current_flag = P (future)
