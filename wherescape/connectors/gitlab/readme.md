# Gitlab connector

Gitlab connector for WhereScape. Takes care of reading projects, issues, tags,
pipelines and merge requests from a Gitlab instance.

# Preparation

## WhereScape Parameters

Add the following parameters to WhereScape:
* `gitlab_base_url`
* `gitlab_access_token`
* `gitlab_high_water_mark`
* `gitlab_high_water_mark_next`

Fill in the correct gitlab_base_url and gitlab_access_token. The water marks
will be filled automatically.

## Load table
Add 5 load tables: one for each of the object types. For the smart scripts to
work it is impotant to give the load tables the right names so the object type
can be derived from the table name. Examples:
* load_gitlab_issue
* load_gitlab_project
* load_gitlab_merge_request
* load_gitlab_pipeline
* load_gitlab_tag

Attaching the right scripts is all thats needed to get it working; no input
in the Source tab is needed.

## Host script
Create the following new python host scripts:

* python_gitlab_create_metadata_smart
* python_gitlab_load_data_smart
* python_gitlab_next_high_water_mark
* python_gitlab_update_high_water_mark

The scripts should utilize the 4 functions with the same name from the python_ files. Example:

```
from wherescape.connectors.gitlab.python_gitlab_create_metadata import gitlab_create_metadata_smart

if __name__ == "__main__":
    gitlab_create_metadata_smart()
```

# Usage

First attach the metadata host script to the load table. After creating the
table, attach the load_data_smart host script to the load table and plan the
job in the scheduler. The incremental function should be active after the
first run.
