"""
COLUMN_NAMES_AND_DATA_TYPES is a dictionary with the flattened values and
belonging data types returned from the Gitlab API
"""

COLUMN_NAMES_AND_DATA_TYPES = {
    "projects": {
        "id": "int",
        "description": "object",
        "name": "object",
        "name_with_namespace": "object",
        "path": "object",
        "path_with_namespace": "object",
        "created_at": "datetime64[ns]",
        "default_branch": "object",
        "ssh_url_to_repo": "object",
        "http_url_to_repo": "object",
        "web_url": "object",
        "readme_url": "object",
        "avatar_url": "object",
        "forks_count": "int",
        "star_count": "int",
        "last_activity_at": "datetime64[ns]",
    },
    "tags": {
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
    },
    "issues": {
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
        "issue_type": "object",
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
    },
    "pipelines": {
        "id": "int",
        "iid": "int",
        "project_id": "int",
        "status": "object",
        "source": "object",
        "ref": "object",
        "sha": "object",
        "web_url": "object",
        "created_at": "datetime64[ns]",
        "updated_at": "datetime64[ns]",
    },
    "merge_requests": {
        "id": "int",
        "iid": "int",
        "project_id": "int",
        "title": "object",
        "description": "object",
        "state": "object",
        "merged_at": "datetime64[ns]",
        "closed_at": "datetime64[ns]",
        "created_at": "datetime64[ns]",
        "updated_at": "datetime64[ns]",
        "target_branch": "object",
        "source_branch": "object",
        "source_project_id": "int",
        "target_project_id": "int",
        "draft": "boolean",
        "work_in_progress": "boolean",
        "milestone_id": "int",
        "milestone_iid": "int",
        "milestone_project_id": "int",
        "milestone_title": "object",
        "milestone_description": "object",
        "milestone_state": "object",
        "milestone_created_at": "datetime64[ns]",
        "milestone_updated_at": "datetime64[ns]",
        "milestone_due_date": "datetime64[ns]",
        "milestone_start_date": "datetime64[ns]",
        "merge_commit_sha": "object",
        "squash_commit_sha": "object",
    },
}
