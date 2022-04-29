import json
import logging
import pandas as pd
import requests

from requests.auth import HTTPBasicAuth

from wherescape.helper_functions import filter_dict, flatten_json


"""
Keys to keep from ... Dictionaries

In the keys to keep object we keep the fields from JIRA we would like to keep.

flattened -> so this response from JIRA:
{'issuetype': {
    description: 'A description of the issuetype'
}}
becomes: issuetype_description: 'A description of the issuetype'.

It is paired together with the python type we want the field to be in, since
the response is all strings. Later on these types can be converted to the DB
type you need.

"""
KEYS_TO_KEEP_FROM_TICKETS_JSON = {
    "id": "int",
    "key": "object",
    "issuetype_id": "int",
    "issuetype_description": "object",
    "issuetype_name": "object",
    "project_id": "int",
    "project_name": "object",
    "timespent": "int",
    "resolutiondate": "datetime64[ns]",
    "created": "datetime64[ns]",
    "priority_name": "object",
    "priority_id": "int",
    "updated": "datetime64[ns]",
    "status_name": "object",
    "status_id": "int",
    "status_statusCategory_id": "int",
    "status_statusCategory_key": "object",
    "status_statusCategory_name": "object",
    "timeoriginalestimate": "int",
    "duedate": "datetime64[ns]",
    "resolution": "object",
    "resolutiondate": "datetime64[ns]",
}

KEYS_TO_KEEP_FROM_PROJECTS_JSON = {
    "self": "object",
    "id": "int",
    "key": "object",
    "name": "object",
    "projectTypeKey": "object",
    "simplified": "boolean",
    "style": "object",
    "isPrivate": "boolean",
    "entityId": "object",
    "uuid": "object",
}


class Jira:
    def __init__(self, user, apikey, base_url):
        self.user = user
        self.apikey = apikey
        self.base_url = base_url

        self.issue_jql = "project = {}"
        self.issue_jql_since = "project = {} AND (created >= {} OR updated >= {})"
        self.search_issues = "/search"
        self.search_projects = "/project/search"

    def issue_column_names_and_types(self):
        """
        Return value is the Dictionary for issues of what we keep from the
        JIRA response.
        """
        return KEYS_TO_KEEP_FROM_TICKETS_JSON

    def project_column_names_and_types(self):
        """
        Return value is the Dictionary for projects of what we keep from the
        JIRA response.
        """
        return KEYS_TO_KEEP_FROM_PROJECTS_JSON

    def make_request(self, url, method, payload={}):
        """
        Function that makes the actual request to JIRA.

        Parameters:
        url (string): The url the request should be made to
        method (string): The request method (e.g. POST GET)
        payload (json): The payload when a POST request is made

        Returns:
        response object: response of the request made
        """
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        auth = HTTPBasicAuth(self.user, self.apikey)
        response = requests.request(
            method, url, data=payload, headers=headers, auth=auth
        )
        if response.status_code != 200:
            raise Exception(
                "JIRA connection error %d: %s"
                % (response.status_code, response.content)
            )
        return response

    def get_all_projects(self, as_numpy=True):
        """
        Get all projects

        Parameters:
        as_numpy (boolean): set to False when you just need the response
        True when you need to have the projects converted to a list of lists

        Returns:
        project data in list of lists
        OR
        response object: response of the request made
        """
        url = f"{self.base_url}{self.search_projects}"

        response = self.make_request(url, "GET")
        json_response = response.json()
        projects = {}

        if as_numpy:
            project_keys_list = KEYS_TO_KEEP_FROM_PROJECTS_JSON.keys()

            for project in json_response["values"]:
                if project["isPrivate"]:
                    continue
                projects[project["id"]] = filter_dict(project, project_keys_list)

            data_as_frame = pd.DataFrame(
                projects.values(),
                index=list(projects.keys()),
                columns=list(project_keys_list),
            )

            data_as_frame = self.clean_dataframe(
                data_as_frame, KEYS_TO_KEEP_FROM_PROJECTS_JSON
            )

            return data_as_frame.values.tolist()
        return json_response["values"]

    def get_all_issues(self, since=None):
        """
        Get all issues

        Parameters:
        since (string): For values accepted check here:
        https://support.atlassian.com/jira-software-cloud/docs/advanced-search-reference-jql-fields/#Created

        Returns:
        all_issues_per_project: list of lists with issue data
        """
        projects = self.get_all_projects(as_numpy=False)

        all_issues_per_project = []
        for project in projects:
            if project["isPrivate"]:
                continue
            all_issues_per_project.extend(
                self.get_issue_data_per_project(project["id"], since)
            )
        return all_issues_per_project

    def get_issue_data_per_project(self, project_id, since=None):
        """
        Get all issue data per project
        Pagination to retrieve all issue per project from the API

        Parameters:
        project_id(string): id of a project provided by response of JIRA API
        since (string): For values accepted check here:

        Returns:
        all_issues: list of issues for one project
        """
        total_of_issues = 1
        max_results = 1
        start_at = 0
        if since:
            jql = self.issue_jql_since.format(project_id, since, since)
        else:
            jql = self.issue_jql.format(project_id)

        all_issues = []

        while start_at <= (total_of_issues - max_results):
            url = f"{self.base_url}{self.search_issues}"

            payload = json.dumps(
                {
                    "jql": jql,
                    "maxResults": max_results,
                    "startAt": start_at,
                }
            )
            response = self.make_request(url, "POST", payload=payload)
            response.raise_for_status()
            json_response = response.json()

            if "errorMessages" in json_response:
                logging.warn(json_response["errorMessages"])
                return []

            total_of_issues = json_response["total"]
            max_results = json_response["maxResults"]
            start_at = json_response["startAt"] + json_response["maxResults"]

            if len(json_response["issues"]) > 0:
                all_issues.extend(self.clean_issue_data(json_response["issues"]))
        return all_issues

    def clean_dataframe(self, dataframe, properties_to_transform):
        """
        Will transform the strings to the types given in the keys to keep dictionary

        Parameters:
        dataframe(object): a pandas dataframe
        properties_to_transform(dict): The dictionary with the keys to keep and their belonging types

        Returns:
        dataframe: a clean dataframe with the correct types
        """
        # Only have one type that is empty: None
        dataframe = dataframe.where(dataframe.notnull(), None)
        for key, value in properties_to_transform.items():
            # to make it a little bit more faster, let's skip object, since it is already an object (string)
            if "object" == value:
                continue
            try:
                dataframe[key] = dataframe[key].astype(value, errors="ignore")
            except KeyError:
                logging.info(
                    key + " key not in dataframe, skipping transforming datatype"
                )
                dataframe[key] = ""  # todo: check if now keys are missing.
        return dataframe

    def clean_issue_data(self, issues):
        """
        Flattens the json response and filters that dictionary for the keys you want to keep
        Will also clean the dataframe transforming the strings into the correct types

        Parameters:
        issues(dict): A dictionary with the issues from the response

        Returns:
        issue_data_in_list: a list of lists with all issue data
        """
        data = {}

        issues_keys_list = KEYS_TO_KEEP_FROM_TICKETS_JSON.keys()

        for issue in issues:
            flattend_dict = flatten_json(json_response=issue, name_to_skip="fields")
            data[issue["id"]] = filter_dict(flattend_dict, issues_keys_list)

        data_as_frame = pd.DataFrame(
            data.values(), list(data.keys()), columns=list(issues_keys_list)
        )
        data_as_frame = self.clean_dataframe(
            data_as_frame, KEYS_TO_KEEP_FROM_TICKETS_JSON
        )
        try:
            issue_data_in_list = data_as_frame.values.tolist()
        except:
            raise
        return issue_data_in_list
