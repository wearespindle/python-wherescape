# Jira connector

Jira connector for WhereScape. Takes care of reading Projects and issues from
a Jira environment.

# Preparation

## WhereScape Parameters

Add the following parameters to WhereScape:
* `jira_user`
* `jira_apikey`
* `jira_high_water_mark`

Fill in the correct jira_user and jira_apikey. The jira_high_water_mark will
be filled automatically.

## Load table
Add 2 load tables: one for projects and one for issues. with the correct Jira
api url in the `Source Directory` field. The url should look like this: 
`https://company.atlassian.net/rest/api/3`.

## Host script
Create a new python host script and add it to the load table. Example code:

```
from wherescape.connectors.jira.python_jira_load_data import jira_load_data_project

jira_load_data_project()
```

Host scripts to create:
* python_jira_create_metadata_project
* python_jira_create_metadata_issue
* python_jira_load_data_project
* python_jira_load_data_issue_full
* python_jira_load_data_issue_incremental

# Usage

First attach the metadata host script to the load table. After creating the
table, attach the load_data host script to the load table. For issues, after
the initial load, use the incremental load for better performance.
