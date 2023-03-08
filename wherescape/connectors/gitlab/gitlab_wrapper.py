"""Module to fetch data (e.g. tickets, projects, pipelines) from the Gitlab API"""
import requests
import logging

from .gitlab_data_types_column_names import COLUMN_NAMES_AND_DATA_TYPES
from ...helper_functions import flatten_json, filter_dict, fill_out_empty_keys


class Gitlab:
    def __init__(self, access_token, base_url, since=None):
        """
        Initializes the Gitlab class. needs the Gitlab access token, base url
        and an optional `since` parameter. Since should be the date from which
        data should be loaded.
        """
        self.access_token = access_token
        self.base_url = base_url
        self.since = since

        # Project IDs are needed to get the other resources as well.
        self.projects = self.get_projects()

    def make_request(self, url, method, params, payload={}):
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
        response = requests.request(
            method,
            f"{self.base_url}/{url}",
            params=params,
            data=payload,
            headers=headers,
        )
        return response

    def paginate_through_resource(
        self,
        resource_api,
        keys_to_keep,
        params={},
        overwrite=None,
        per_page=50,
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
        since (string): ISO formatted datetime string to indicate since which date you want values back (e.g. 2022-09-20T08:29:21)
        overwrite (dict): A dictionary with a key, value pair to overwrite the none value with a fixed value

        Returns:
        List of tuples with the values from the request

        """
        next_page = "1"

        all_resources = []

        while len(next_page) != 0:
            page_variables = {"per_page": per_page, "page": next_page}
            params.update(page_variables)

            response = self.make_request(resource_api, "GET", params=params)

            if response.status_code == 403:
                logging.info(
                    f"{resource_api}\n Forbidden resource. If you need this resource, please check the user's rights"
                )
                break

            response.raise_for_status()

            json_response = response.json()

            for resource_object in json_response:
                cleaned_json = filter_dict(flatten_json(resource_object), keys_to_keep)
                final_json = fill_out_empty_keys(cleaned_json, keys_to_keep, overwrite)
                all_resources.append(list(final_json.values()))

            next_page = response.headers.get("X-Next-Page", "")

        return all_resources

    def get_projects_loaded(self):
        """Get projects

        Returns:
        List of tuples with the project values
        """
        return self.projects

    def get_projects(self):
        """Get projects from API

        Returns:
        List of tuples with the project values from the API
        """
        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["projects"].keys()
        resource_api = "projects"

        params = {
            "order_by": "id",
            "updated_after": self.since,
            "sort": "asc",
        }

        all_projects = self.paginate_through_resource(
            resource_api, keys_to_keep, params
        )
        return all_projects

    def get_release_tags(self):
        """Get release tags

        Returns:
        List of tuples with the tags values from the API
        """
        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["tags"].keys()

        all_tags = []

        params = {
            "order_by": "name",
            "updated_after": self.since,
            "sort": "asc",
        }

        for project in self.projects:
            project_id = project[0]
            # tags don't have a project_id in the response so we add it here
            overwrite = {"project_id": project_id}

            resource_api = f"projects/{project_id}/repository/tags"
            tag_in_tuple = self.paginate_through_resource(
                resource_api,
                keys_to_keep,
                params,
                overwrite=overwrite,
            )

            all_tags.extend(tag_in_tuple)

        return all_tags

    def get_issues(self):
        """Get issues

        Returns:
        List of tuples with the issues values from the API
        """
        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["issues"].keys()

        all_issues = []

        params = {
            "order_by": "created_at",
            "updated_after": self.since,
            "sort": "asc",
        }
        # projects is a list of tuples, so the first item in the tuple is the id
        for project in self.projects:
            project_id = project[0]
            resource_api = f"projects/{project_id}/issues"

            project_issues = self.paginate_through_resource(
                resource_api, keys_to_keep, params
            )
            all_issues.extend(project_issues)

        return all_issues

    def get_pipelines(self):
        """Get pipelines

        Returns:
        List of tuples with the pipelines values from the API
        """
        all_pipelines = []

        params = {
            "order_by": "id",
            "updated_after": self.since,
            "sort": "asc",
        }

        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["pipelines"].keys()
        # projects is a list of tuples, so the first item in the tuple is the id
        for project in self.projects:
            project_id = project[0]
            resource_api = f"projects/{project_id}/pipelines"
            project_pipelines = self.paginate_through_resource(
                resource_api, keys_to_keep, params
            )
            all_pipelines.extend(project_pipelines)

        return all_pipelines

    def get_merge_requests(self):
        """Get merge requests

        Returns:
        List of tuples with the merge request values from the API
        """
        all_merge_requests = []

        params = {
            "order_by": "title",
            "updated_after": self.since,
            "sort": "asc",
        }

        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["merge_requests"].keys()
        # projects is a list of tuples, so the first item in the tuple is the id
        for project in self.projects:
            project_id = project[0]
            resource_api = f"projects/{project_id}/merge_requests"
            project_merge_requests = self.paginate_through_resource(
                resource_api, keys_to_keep, params
            )
            all_merge_requests.extend(project_merge_requests)

        return all_merge_requests

    def get_commits(self):
        """Get commits

        Returns:
        List of tuples with the commits values from the API
        """
        all_commits = []

        params = {
            "order": "default",
            "since": self.since,
            "sort": "asc",
        }

        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["commits"].keys()
        # tags don't have a project_id in the response so we add it here

        # projects is a list of tuples, so the first item in the tuple is the id
        for project in self.projects:
            project_id = project[0]
            overwrite = {"project_id": project_id}
            resource_api = f"projects/{project_id}/repository/commits"
            project_commits = self.paginate_through_resource(
                resource_api,
                keys_to_keep,
                params,
                overwrite=overwrite,
            )
            all_commits.extend(project_commits)

        return all_commits

    def get_merge_request_commits(self):
        """Get merge request commits

        Returns:
        List of tuples with the commits values from the API
        """
        all_merge_requests = self.get_merge_requests()

        all_commits = []

        params = {
            "order": "default",
            "since": self.since,
            "sort": "asc",
        }

        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["merge_request_commits"].keys()

        for merge_request in all_merge_requests:
            mr_iid = merge_request[1]
            project_id = merge_request[2]
            overwrite = {"project_id": project_id, "merge_request_iid": mr_iid}

            resource_api = f"projects/{project_id}/merge_requests/{mr_iid}/commits"
            merge_request_commits_commits = self.paginate_through_resource(
                resource_api,
                keys_to_keep,
                params,
                overwrite=overwrite,
            )
            all_commits.extend(merge_request_commits_commits)

        return all_commits

    def get_commits_with_branch_name(self):
        """Get commits

        Returns:
        List of tuples with the commits values from the API
        """
        all_commits = []

        params = {
            "order_by": "default",
            "updated_after": self.since,
            "sort": "asc",
        }

        for project in self.projects:
            project_id = project[0]

            resource_api = f"projects/{project_id}/repository/branches"

            keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["branches"].keys()
            project_branches = self.paginate_through_resource(
                resource_api,
                keys_to_keep,
            )

            for branch in project_branches:
                branch_name = branch[1]
                overwrite = {"branch_name": branch_name, "project_id": project_id}
                keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["branch_commits"].keys()
                params = {
                    "order": "default",
                    "since": self.since,
                    "ref_name": branch_name,
                }
                resource_api = f"projects/{project_id}/repository/commits"
                project_commits = self.paginate_through_resource(
                    resource_api,
                    keys_to_keep,
                    params,
                    overwrite=overwrite,
                )
                all_commits.extend(project_commits)

        return all_commits

    def get_branches(self):
        """Get branches

        Returns:
        List of tuples with the branches of the specific projects from the API
        """
        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["branches"].keys()
        all_branches = []

        for project in self.projects:
            project_id = project[0]
            overwrite = {"project_id": project_id}
            resource_api = f"projects/{project_id}/repository/branches"
            project_branches = self.paginate_through_resource(
                resource_api,
                keys_to_keep,
                overwrite=overwrite,
            )
            all_branches.extend(project_branches)

        return all_branches
