import pandas as pd
import logging
import requests
import os

from requests.auth import HTTPBasicAuth

from wherescape.helper_functions import filter_dict, flatten_json


# these are the already flattened keys to avoid having to loop into dicts in dicts.
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
    # "expand": "object",
    "self": "object",
    "id": "int",
    "key": "object",
    "name": "object",
    "projectTypeKey": "object",
    "simplified": "boolean",
    "style": "object",
    "isPrivate": "boolean",
    # "properties": "object",
    "entityId": "object",
    "uuid": "object",
}


class Jira:
    def __init__(self, user, apikey, base_url):
        self.user = user
        self.apikey = apikey
        self.base_url = base_url
        self.search_issues = "/search?jql=project={}&startAt={}&maxResults=1"
        self.search_projects = "/project/search"

    def issue_column_names_and_types(self):
        return KEYS_TO_KEEP_FROM_TICKETS_JSON

    def project_column_names_and_types(self):
        return KEYS_TO_KEEP_FROM_PROJECTS_JSON

    def make_request(self, url, method):
        headers = {"Accept": "application/json"}

        auth = HTTPBasicAuth(self.user, self.apikey)

        response = requests.request(method, url, headers=headers, auth=auth)
        return response

    def get_all_projects(self, as_numpy=True):
        url = f"{self.base_url}{self.search_projects}"
        logging.info(url)

        response = self.make_request(url, "GET")
        # logging.info(response)
        # logging.info(response.content)

        json_response = response.json()
        projects = {}

        if as_numpy:
            for project in json_response["values"]:
                projects[project["id"]] = filter_dict(
                    project, KEYS_TO_KEEP_FROM_PROJECTS_JSON.keys()
                )

            data_as_frame = pd.DataFrame(projects).transpose()
            data_as_frame = self.clean_dataframe(
                data_as_frame, KEYS_TO_KEEP_FROM_PROJECTS_JSON
            )

            projects_in_tuples = data_as_frame.to_records(index=False)
            return list(projects_in_tuples)
        return json_response["values"]

    def get_all_issues(self):
        projects = self.get_all_projects(as_numpy=False)

        all_issues_per_project = []
        for project in projects:
            issue_url = self.base_url + self.search_issues.format(project["id"], 0)
            all_issues_per_project.extend(
                self.get_issue_data_per_project(project["id"], issue_url)
            )
            break
        return all_issues_per_project

    def get_issue_data_per_project(self, project_id, url):
        total_of_issues = 1
        max_results = 1
        start_at = 0

        all_issues = []

        while start_at <= (total_of_issues - max_results):
            url = self.base_url + self.search_issues.format(project_id, start_at)

            response = self.make_request(url, "GET")
            json_response = response.json()
            # logging.info(json_response)
            # total_of_issues = json_response["total"]
            max_results = json_response["maxResults"]
            start_at = json_response["startAt"] + json_response["maxResults"]

            all_issues.extend(self.clean_issue_data(json_response))
            break
        return all_issues

    def clean_dataframe(self, dataframe, properties_to_transform):
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

    def clean_issue_data(self, json_response):
        data = {}

        if json_response["issues"]:
            for issue in json_response["issues"]:
                flattend_dict = flatten_json(json_response=issue, name_to_skip="fields")
                new_dict = filter_dict(
                    flattend_dict, KEYS_TO_KEEP_FROM_TICKETS_JSON.keys()
                )
                data[new_dict["id"]] = new_dict
            data_as_frame = pd.DataFrame(data).transpose()

            data_as_frame = self.clean_dataframe(
                data_as_frame, KEYS_TO_KEEP_FROM_TICKETS_JSON
            )
            try:
                tickets_in_tuples = data_as_frame.values.tolist()
            except:
                raise
            return tickets_in_tuples
