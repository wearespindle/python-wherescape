from textwrap import indent
import requests
import json

from wherescape.helper_functions import flatten_json, filter_dict

PROJECTS_KEYS_TO_DELETE = ["namespace"]

PROJECTS_COLUMN_NAMES_DATA_TYPES = {
    "id": "int",
    "description": "object",
    "name": "object",
    "name_with_namespace": "object",
    "path": "object",
    "path_with_namespace": "object",
    "created_at": "datetime64[ns]",
    "default_branch": "object",
    "tag_list": "object",
    "topics": "object",
    "ssh_url_to_repo": "object",
    "http_url_to_repo": "object",
    "web_url": "object",
    "readme_url": "object",
    "avatar_url": "object",
    "forks_count": "int",
    "star_count": "int",
    "last_activity_at": "datetime64[ns]",
}

# Already flattened
TAGS_COLUMN_NAMES_DATA_TYPES = {
    "name": "object",
    "message": "object",
    "target": "object",
    "commit_id": "object",
    "commit_short_id": "object",
    "commit_created_at": "datetime64[ns]",
    "commit_parent_ids_0": "object",
    "commit_parent_ids_1": "object",
    "commit_title": "object",
    "commit_message": "object",
    "commit_author_name": "object",
    "commit_author_email": "object",
    "commit_authored_date": "datetime64[ns]",
    "commit_committer_name": "object",
    "commit_committer_email": "object",
    "commit_committed_date": "datetime64[ns]",
    "commit_web_url": "object",
    "release_tag_name": "object",
    "release_description": "object",
    "protected": "boolean",
}

ISSUES_COLUMN_NAMES_DATA_TYPES = {
    "id": "int",
    "iid": "int",
    "project_id": "int",
    "title": "object",
    "description": "object",
    "state": "object",
    "created_at": "datetime64[ns]",
    "updated_at": "datetime64[ns]",
    "closed_at": "datetime64[ns]",
    "milestone_id": "int",
    "milestone_iid": "int",
    "milestone_group_id": "int",
    "milestone_title": "object",
    "milestone_description": "object",
    "milestone_state": "object",
    "milestone_created_at": "datetime64[ns]",
    "milestone_updated_at": "datetime64[ns]",
    "milestone_due_date": "datetime64[ns]",
    "milestone_start_date": "datetime64[ns]",
    "milestone_expired": "boolean",
    "milestone_web_url": "object",
    "type": "object",
    "user_notes_count": "int",
    "merge_requests_count": "int",
    "upvotes": "int",
    "downvotes": "int",
    "due_date": "datetime64[ns]",
    "confidential": "boolean",
    "discussion_locked": "boolean",
    "issue_type": "issue",
    "web_url": "object",
    "time_stats_time_estimate": "int",
    "time_stats_total_time_spent": "int",
    "time_stats_human_time_estimate": "object",
    "time_stats_human_total_time_spent": "object",
    "task_completion_status_count": "int",
    "task_completion_status_completed_count": "int",
    "weight": "int",
    "blocking_issues_count": "int",
    "has_tasks": "boolean",
    "references_short": "object",
    "references_relative": "object",
    "references_full": "object",
    "moved_to_id": "object",
    "service_desk_reply_to": "object",
    "epic_iid": "int",
    "epic_id": "int",
    "epic_title": "object",
    "epic_url": "object",
    "epic_group_id": "int",
    "epic_human_readable_end_date": "object",
    "epic_human_readable_timestamp": "object",
}


class Gitlab:
    def __init__(self, access_token, base_url):
        self.access_token = access_token
        self.base_url = base_url

    # get projects

    # get tags -> equals deployments, maybe the info gives you back if it's an release or not

    # maybe skip some projects, since we have a lot of projects that we don't need tags from?
    # maybe we also don't need all tickets from projects, so we need to skip those as well.

    # deploys of infra and voipgrid are coupled to eachother

    # not every project needs to be deployed

    # get issues because mobile is still working with issues

    def make_tuples(self):

        pass

    def project_column_names_and_types(self):
        return PROJECTS_COLUMN_NAMES_DATA_TYPES

    def tags_column_names_and_types(self):
        return TAGS_COLUMN_NAMES_DATA_TYPES

    def make_request(self, url, method, payload={}):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self.access_token),
        }
        response = requests.request(method, url, data=payload, headers=headers)
        return response

    def get_projects(self, per_page, project_ids=[]):

        total_pages = 1
        current_page = 0

        all_projects = []

        while current_page < total_pages:
            url = f"{self.base_url}/projects?simple=true&per_page={per_page}&page={int(current_page)+1}"

            response = self.make_request(url, "GET")
            response.raise_for_status()

            json_response = response.json()  # ONTHOUD -> dit is een list
            for project in json_response:
                print(project["id"])
                for key in PROJECTS_KEYS_TO_DELETE:
                    del project[key]
                all_projects.append(tuple(project.values()))

            total_pages = response.headers["X-Total-Pages"]
            current_page = response.headers["X-Page"]
            break
        return all_projects

        # print(json.dumps(projects_response, indent=4))

    def get_release_tags(self, projects):

        all_tags = []

        for project in projects:
            total_pages = 1
            current_page = 0

            # projects is a list of tuples, so the first item in the tuple is the id
            project_id = project[0]

            while current_page < total_pages:
                url = f"{self.base_url}/projects/{project_id}/repository/tags?page={int(current_page)+1}"
                response = self.make_request(url, "GET")

                total_pages = response.headers["X-Total-Pages"]
                current_page = response.headers["X-Page"]
                json_response = response.json()
                # ONTHOUD -> dit is een list
                for tag in json_response:
                    tag_in_tuple = tuple(flatten_json(tag).values())
                    all_tags.append(tag_in_tuple)

            return all_tags

    def get_issues(self, projects):
        total_pages = 1
        current_page = 0

        all_issues = []

        issues_keys_list = ISSUES_COLUMN_NAMES_DATA_TYPES.keys()
        # projects is a list of tuples, so the first item in the tuple is the id
        for project in projects:
            project_id = project[0]
            while current_page < total_pages:
                url = f"{self.base_url}/projects/{project_id}/issues?page={int(current_page)+1}"
                response = self.make_request(url, "GET")

                total_pages = response.headers["X-Total-Pages"]
                current_page = response.headers["X-Page"]

                json_response = response.json()
                for issue in json_response:
                    cleaned_json = filter_dict(
                        flatten_json(issue), keys_to_keep=issues_keys_list
                    )
                    issue_in_tuple = tuple(cleaned_json.values())
                    all_issues.append(issue_in_tuple)
        return all_issues
