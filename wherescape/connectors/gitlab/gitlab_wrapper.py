"""Module to fetch data (e.g. tickets, projects, pipelines) from the Gitlab API"""
import requests
import logging

from ...helper_functions import flatten_json, filter_dict, fill_out_empty_keys

"""COLUMN_NAMES_AND_DATA_TYPES is a dictionary with the flattened values and belonging data types returned from the Gitlab API """
from ...connectors.gitlab.gitlab_data_types_column_names import (
    COLUMN_NAMES_AND_DATA_TYPES,
)


class Gitlab:
    def __init__(self, access_token, base_url, since):
        self.access_token = access_token
        self.base_url = base_url
        self.since = since

        """Project IDs are needed to get the other resources as well."""
        self.projects = self.get_projects_from_api()

    def make_request(self, url, method, payload={}):
        """Make request

        Parameters:
        url (string): The url the request should be made to
        method (string): The request method (e.g. POST GET)
        payload (json): The payload when a POST request is made

        Returns:
        response object: response of the request made
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }
        response = requests.request(method, url, data=payload, headers=headers)
        return response

    def format_url(self, resource_api, page_variables, simple, order_by, since):
        """Format URL

        Parameters:
        resource_api (string): The location of the resource requested
        page_variables (object): {
            "per_page": string or int,
            "current_page": string or int
        }
        simple (boolean): If the response of Gitlab should be simplified this needs to be set on True
        since (string): ISO formatted datetime string to indicate since which date you want values back

        Returns:
        Formatted url which can be used to make the request
        """
        updated_since = f"&updated_after={since}" if since else ""
        pagination = f"per_page={page_variables['per_page']}&page={int(page_variables['current_page'])+1}"
        return f"{self.base_url}/{resource_api}?order_by={order_by}&sort=asc&simple={simple}&{pagination}{updated_since}"

    def paginate_through_resource(
        self,
        resource_api,
        keys_to_keep,
        since=None,
        per_page=50,
        simple="false",
        order_by="id",
    ):
        """Paginate through resources
        Since the Gitlab API has pagination, this helper function will paginate through the resource API.
        It will do that until all responses are collected.
        It cleans the response immediately and turns it into a tuple.
        It does expect a list with objects in the response of the API.

        Parameters:
        resource_api (string): The location of the resource requested
        keys_to_keep (list): List of keys returned by the API you want to keep
        per_page (int): How many results per page you would like to get
        simple (boolean): If the response of Gitlab should be simplified this needs to be set on True
        since (string): ISO formatted datetime string to indicate since which date you want values back

        Returns:
        List of tuples with the values from the request

        """
        total_pages = 1
        current_page = 0

        all_resources = []

        while current_page < total_pages:
            page_variables = {"per_page": per_page, "current_page": current_page}
            url = self.format_url(resource_api, page_variables, simple, order_by, since)

            response = self.make_request(url, "GET")
            response.raise_for_status()

            if response.status_code == 403:
                logging.warn(
                    f"{url} \n Forbidden resource, please check the user's rights"
                )
                current_page = current_page + 1
                continue

            json_response = response.json()

            for resource_object in json_response:
                cleaned_json = filter_dict(flatten_json(resource_object), keys_to_keep)
                final_json = fill_out_empty_keys(cleaned_json, keys_to_keep)
                all_resources.append(list(final_json.values()))

            try:
                total_pages = response.headers["X-Total-Pages"]
                current_page = response.headers["X-Page"]
            except:
                current_page = current_page + 1

        return all_resources

    def get_projects(self):
        return self.projects

    def get_projects_from_api(self):
        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["projects"].keys()
        resource_api = "projects"

        all_projects = self.paginate_through_resource(
            resource_api, keys_to_keep, simple="true"
        )
        return all_projects

    def get_release_tags(self):

        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["tags"].keys()

        all_tags = []

        for project in self.projects:
            project_id = project[0]

            resource_api = f"projects/{project_id}/repository/tags"
            tag_in_tuple = self.paginate_through_resource(
                resource_api, keys_to_keep, order_by="name", since=self.since
            )

            all_tags.extend(tag_in_tuple)

        return all_tags

    def get_issues(self):
        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["issues"].keys()

        all_issues = []
        # projects is a list of tuples, so the first item in the tuple is the id
        for project in self.projects:
            project_id = project[0]
            resource_api = f"projects/{project_id}/issues"

            project_issues = self.paginate_through_resource(
                resource_api, keys_to_keep, order_by="created_at", since=self.since
            )
            all_issues.extend(project_issues)

        return all_issues

    def get_pipelines(self):

        all_pipelines = []

        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["pipelines"].keys()
        # projects is a list of tuples, so the first item in the tuple is the id
        for project in self.projects:
            project_id = project[0]
            resource_api = f"projects/{project_id}/pipelines"
            project_pipelines = self.paginate_through_resource(
                resource_api, keys_to_keep, since=self.since
            )
            all_pipelines.extend(project_pipelines)

        return all_pipelines

    def get_merge_requests(self):

        all_merge_requests = []

        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["merge_requests"].keys()
        # projects is a list of tuples, so the first item in the tuple is the id
        for project in self.projects:
            project_id = project[0]
            resource_api = f"projects/{project_id}/merge_requests"
            project_merge_requests = self.paginate_through_resource(
                resource_api, keys_to_keep, since=self.since, order_by="title"
            )
            all_merge_requests.extend(project_merge_requests)

        return all_merge_requests
