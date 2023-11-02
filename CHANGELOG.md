# Changelog

| __Date__    |__Change__ |
|------------:|:-|
| 2021-12-13  | Initial version |
| 2022-10-04  | Added a 'validator' python module that contains a function which generates a report with record counts for all facts, and counts for all dimension keys of those facts, having value 0 as dimension key |
| 2022-06-16  | JIRA connector initial version was added |
| 2022-07-27  | GITLAB connector initial version was added |
| 2022-09-01  | The year month day was added to the logging |
| 2022-09-27  | Added file log rotation |
| 2022-10-04  | Add fact dimension join check |
| 2022-10-12  | Retrieving commits from the Gitlab API was added |
| 2022-10-27  | Add commits and branches to gitlab import |
| 2022-11-04  | Process review wherescape OS |
| 2022-11-16  | Adjust pagination of Gitlab wrapper |
| 2023-01-23  | Retrieving commits per branch in Gitlab wrapper, refactored format url function |
| 2023-01-25  | Add function to retrieve commits on a branch, Gitlab wrapper, Add commits on branch in load script of Gitlab wrapper |
| 2023-03-08  | Add merge request commits to the Gitlab wrapper |
| 2023-04-13  | Add date of when ticket was put into progress to Jira wrapper |
| 2023-04-20  | Improved loggging when not run via scheduler |
| 2023-04-20  | In the Jira wrapper, when a date is None it will not get converted to a datetime |
| 2023-05-32  | Added logging to fact_dimension_join |
| 2023-11-02  | Added Hubspot missing columns check feature to the Hubspot connector |
