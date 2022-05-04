import requests
import logging

from wherescape.helper_functions import flatten_json, filter_dict

from wherescape.connectors.gitlab.gitlab_data_types_column_names import (
    COLUMN_NAMES_AND_DATA_TYPES,
)


class Gitlab:
    def __init__(self, access_token, base_url):
        self.access_token = access_token
        self.base_url = base_url

        self.projects = self.get_projects_from_api()

    def make_request(self, url, method, payload={}):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }
        response = requests.request(method, url, data=payload, headers=headers)
        return response

    def format_url(self, resource_api, page_variables, simple):
        return f"{self.base_url}/{resource_api}?simple={simple}&per_page={page_variables['per_page']}&page={int(page_variables['current_page'])+1}"

    def paginate_through_resource(
        self, resource_api, keys_to_keep, per_page=50, simple="false"
    ):
        total_pages = 1
        current_page = 0

        all_resources = []

        while current_page < total_pages:
            page_variables = {"per_page": per_page, "current_page": current_page}
            url = self.format_url(resource_api, page_variables, simple)

            response = self.make_request(url, "GET")

            if response.status_code == 403:
                logging.warn(
                    f"{url} \n Forbidden resource, please check the user's rights"
                )
                current_page = current_page + 1
                continue

            json_response = response.json()

            for resource_object in json_response:
                cleaned_json = filter_dict(flatten_json(resource_object), keys_to_keep)
                all_resources.append(tuple(cleaned_json.values()))

            try:
                total_pages = response.headers["X-Total-Pages"]
                current_page = response.headers["X-Page"]
            except:
                current_page = current_page + 1

            break

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
            tag_in_tuple = self.paginate_through_resource(resource_api, keys_to_keep)

            all_tags.extend(tag_in_tuple)

        return all_tags

    def get_issues(self):
        keys_to_keep = COLUMN_NAMES_AND_DATA_TYPES["issues"].keys()

        all_issues = []
        # projects is a list of tuples, so the first item in the tuple is the id
        for project in self.projects:
            project_id = project[0]
            resource_api = f"projects/{project_id}/issues"

            project_issues = self.paginate_through_resource(resource_api, keys_to_keep)
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
                resource_api, keys_to_keep
            )
            all_pipelines.extend(project_pipelines)

        return all_pipelines
