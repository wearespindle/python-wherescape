"""
Wrapper for Friday Pulse API integration.

This module integrates the Friday Pulse client code directly and provides
functions to fetch and flatten results data for warehouse loading.
"""

import logging
from typing import Any

import requests

from ...helper_functions import flatten_json


class FridayPulseClient:
    """Client for interacting with the FridayPulse API.

    The class facilitates communication with the FridayPulse service via HTTP
    requests. It enables users to authenticate using a bearer token and interact
    with specific API endpoints such as retrieving available result dates and
    fetching results for a given date.
    """

    def __init__(self, bearer_token: str):
        """Initialize the client with a bearer token for authentication.

        Args:
            bearer_token: The bearer token for API authentication
        """
        self._bearer_token = bearer_token

    # Private helper methods

    def _request(self, url: str, timeout: int = 30) -> Any:
        """Send a GET request to the specified URL.

        The request incorporates an Authorization header containing a Bearer token.
        Returns the JSON response received from the external service.

        Args:
            url: The URL path to append to the base URL
            timeout: Request timeout in seconds (default: 30)

        Returns:
            JSON response from the API
        """
        response = requests.get(
            "https://app.fridaypulse.com/" + url,
            headers={"Authorization": f"Bearer {self._bearer_token}"},
            timeout=timeout,
        )
        response.raise_for_status()

        return response.json()

    def _fetch_and_flatten_list(self, url: str, description: str) -> list[dict]:
        """Fetch a list from the API and flatten each item.

        Args:
            url: The API endpoint URL
            description: Description for logging (e.g., "topics", "group types")

        Returns:
            List of flattened dictionaries
        """
        logging.info(f"Fetching all {description}...")
        items = self._request(url)
        logging.info(f"Retrieved {len(items)} {description}")

        flattened_items = []
        for item in items:
            flattened = flatten_json(item)
            flattened_items.append(flattened)

        return flattened_items

    def _filter_dates_by_since(self, result_dates: list[dict], since_date: str = None) -> list[dict]:
        """Filter result dates by since_date if provided.

        Args:
            result_dates: List of date dictionaries with 'date' key
            since_date: Optional date string (YYYY-MM-DD) to filter after

        Returns:
            Filtered list of dates
        """
        if since_date:
            filtered = [rd for rd in result_dates if rd["date"] > since_date]
            logging.info(f"Filtered to {len(filtered)} dates after {since_date} (from {len(result_dates)} total)")
            return filtered
        return result_dates

    def _flatten_result(self, result: dict, question_count: int) -> dict:
        """Flatten the nested result structure into a flat dictionary.

        Args:
            result: Dictionary containing the raw result data
            question_count: Number of questions for this date

        Returns:
            Flattened dictionary with all nested fields promoted to top level
        """
        # Extract topic fields
        topic = result.get("topic", {}) or {}
        topic_data = {
            "topic_code": topic.get("code"),
            "topic_name": topic.get("name"),
        }

        # Combine all fields into a flat structure
        flat_result = {
            "sample_date": result.get("sample_date"),
            "score": result.get("score"),
            "response_rate": result.get("response_rate"),
            "response_count": result.get("response_count"),
            "total_count": result.get("total_count"),
            "question_count": question_count,
        }

        # Merge topic data
        flat_result.update(topic_data)

        return flat_result

    def _fetch_group_data_by_dates(
        self,
        group_ids: list,
        result_dates: list[dict],
        url_template: str,
        data_type: str,
    ) -> list[dict]:
        """Fetch data for groups across multiple dates.

        Args:
            group_ids: List of group IDs to fetch data for
            result_dates: List of date dictionaries with 'date' key
            url_template: URL template with {group_id} and {date} placeholders
            data_type: Description of data type for logging (e.g., "results", "notes")

        Returns:
            List of flattened dictionaries with group_id added
        """
        all_data = []
        for date_idx, result_date in enumerate(result_dates, 1):
            date = result_date["date"]
            logging.info(f"Processing date {date_idx}/{len(result_dates)}: {date}")

            for group_idx, group_id in enumerate(group_ids, 1):
                try:
                    # Build API URL with date parameter
                    url = url_template.format(group_id=group_id, date=date)

                    # Get data for this group and date from API
                    items = self._request(url)

                    if items:
                        logging.debug(f"  Group {group_idx}/{len(group_ids)} ({group_id}): {len(items)} {data_type}")

                    # Flatten each item and add group_id
                    for item in items:
                        flattened = flatten_json(item)
                        flattened["group_id"] = group_id
                        all_data.append(flattened)

                except requests.exceptions.Timeout:
                    logging.warning(f"  Timeout retrieving {data_type} for group {group_id} on {date}, skipping...")
                    continue
                except requests.exceptions.RequestException as e:
                    logging.warning(f"  Error retrieving {data_type} for group {group_id} on {date}: {e}, skipping...")
                    continue

        return all_data

    # Public API methods - Simple data retrieval

    def get_topics(self) -> list[dict]:
        """Get all topics from Friday Pulse API.

        Returns:
            List of flattened topic dictionaries
        """
        return self._fetch_and_flatten_list("api/v1/topics", "topics")

    def get_group_types(self) -> list[dict]:
        """Get all group types from Friday Pulse API.

        Returns:
            List of flattened group type dictionaries
        """
        return self._fetch_and_flatten_list("api/v1/group-types", "group types")

    def get_groups(self) -> list[dict]:
        """Get all groups from all group types from Friday Pulse API.

        Returns:
            List of flattened group dictionaries (includes group_type_code for each)
        """
        # First get all group types
        logging.info("Fetching group types to get groups...")
        group_types = self._request("api/v1/group-types")
        logging.info(f"Found {len(group_types)} group types")

        # Fetch groups for each group type
        all_groups = []
        for idx, group_type in enumerate(group_types, 1):
            group_type_code = group_type.get("code")
            if not group_type_code:
                logging.warning(f"Group type {idx} has no code, skipping")
                continue

            logging.debug(f"Fetching groups for group type: {group_type_code}")

            try:
                groups = self._request(f"api/v1/group-types/{group_type_code}/groups")
                logging.debug(f"  Retrieved {len(groups)} groups for {group_type_code}")

                # Flatten each group and add group_type_code
                for group in groups:
                    flattened = flatten_json(group)
                    flattened["group_type_code"] = group_type_code
                    all_groups.append(flattened)

            except requests.exceptions.RequestException as e:
                logging.error(f"  Error retrieving groups for {group_type_code}: {e}, skipping...")
                continue

        logging.info(f"Completed: retrieved {len(all_groups)} total groups")
        return all_groups

    def get_results_dates(self) -> list[dict]:
        """Retrieve the available results dates from the API.

        Returns:
            List of dictionaries with 'date' and 'question_count' keys
        """
        response = self._request("api/v1/info/results-dates")
        return response

    # Public API methods - Complex data retrieval with date filtering

    def get_general_results(self, since_date: str = None) -> list[dict]:
        """Get all general results from Friday Pulse, optionally filtered by date.

        This function fetches all available result dates and retrieves results for each date.
        Results are flattened for easy database insertion.

        Args:
            since_date: Optional date string (YYYY-MM-DD) to filter results created after this date.
                       If None, fetches results without date filtering from the API.

        Returns:
            List of flattened dictionaries containing result data, ready for database insertion
        """
        all_results = []

        # If no since_date provided, fetch all results without date parameter
        if since_date is None:
            logging.info("Fetching all results without date filter...")
            try:
                results = self._request("api/v1/results")
                logging.info(f"Retrieved {len(results)} total results")

                for result in results:
                    flattened = self._flatten_result(result, result.get("question_count", 0))
                    all_results.append(flattened)

            except requests.exceptions.RequestException as e:
                logging.error(f"Error retrieving results: {e}")

        else:
            # Get all available result dates
            logging.info("Fetching available result dates...")
            result_dates = self.get_results_dates()
            logging.info(f"Found {len(result_dates)} result dates")

            # Filter dates by since_date
            result_dates = [rd for rd in result_dates if rd["date"] > since_date]
            logging.info(f"Filtered to {len(result_dates)} dates after {since_date}")

            # Fetch results for each date
            for idx, result_date in enumerate(result_dates, 1):
                date = result_date["date"]
                question_count = result_date["question_count"]

                logging.info(f"Processing date {idx}/{len(result_dates)}: {date} ({question_count} questions)")

                try:
                    # Get results for this date from API
                    results = self._request(f"api/v1/results?date={date}")
                    logging.info(f"  Retrieved {len(results)} results for {date}")

                    # Flatten each result and add question_count
                    for result in results:
                        flattened = self._flatten_result(result, question_count)
                        all_results.append(flattened)

                except requests.exceptions.Timeout:
                    logging.error(f"  Timeout retrieving results for {date}, skipping...")
                    continue
                except requests.exceptions.RequestException as e:
                    logging.error(f"  Error retrieving results for {date}: {e}, skipping...")
                    continue

        logging.info(f"Completed: retrieved {len(all_results)} total results")
        return all_results

    def get_group_results(self, since_date: str = None) -> list[dict]:
        """Get results for all groups from Friday Pulse, optionally filtered by date.

        This function fetches all available result dates, then retrieves results for each
        group for each date. Results are flattened for easy database insertion.

        Args:
            since_date: Optional date string (YYYY-MM-DD) to filter results created after this date

        Returns:
            List of flattened dictionaries containing group result data, ready for database insertion
        """
        all_results = []

        # Get all groups
        logging.info("Fetching all groups for results...")
        all_groups = self.get_groups()
        group_ids = [g.get("id") for g in all_groups if g.get("id")]
        logging.info(f"Found {len(group_ids)} groups total")

        # If no since_date provided, fetch latest results without date parameter
        if since_date is None:
            logging.info("Fetching latest results for all groups without date filter...")

            for group_idx, group_id in enumerate(group_ids, 1):
                try:
                    # Get latest results for this group (no date parameter)
                    url = f"api/v1/groups/{group_id}/results"
                    items = self._request(url)

                    if items:
                        logging.debug(f"  Group {group_idx}/{len(group_ids)} ({group_id}): {len(items)} results")

                    # Flatten each item and add group_id
                    for item in items:
                        flattened = flatten_json(item)
                        flattened["group_id"] = group_id
                        all_results.append(flattened)

                except requests.exceptions.Timeout:
                    logging.warning(f"  Timeout retrieving results for group {group_id}, skipping...")
                    continue
                except requests.exceptions.RequestException as e:
                    logging.warning(f"  Error retrieving results for group {group_id}: {e}, skipping...")
                    continue

            logging.info(f"Completed: retrieved {len(all_results)} total results from {len(group_ids)} groups")

        else:
            # Get and filter result dates
            logging.info("Fetching available result dates...")
            result_dates = self.get_results_dates()
            logging.info(f"Found {len(result_dates)} result dates")
            result_dates = self._filter_dates_by_since(result_dates, since_date)

            # Fetch results for each group for each date
            all_results = self._fetch_group_data_by_dates(
                group_ids, result_dates, "api/v1/groups/{group_id}/results?date={date}", "results"
            )

            logging.info(
                f"Completed: retrieved {len(all_results)} total results from "
                f"{len(group_ids)} groups across {len(result_dates)} dates"
            )

        return all_results

    def get_general_notes(self, since_date: str = None) -> list[dict]:
        """Get all general notes from Friday Pulse, optionally filtered by date.

        This function fetches notes that are not associated with specific groups.
        Notes are flattened for easy database insertion.

        Args:
            since_date: Optional date string (YYYY-MM-DD) to filter notes created after this date.
                       If None, fetches notes without date filtering from the API.

        Returns:
            List of flattened dictionaries containing note data, ready for database insertion
        """
        all_notes = []

        # If no since_date provided, fetch all notes without date parameter
        if since_date is None:
            logging.info("Fetching all notes without date filter...")
            try:
                notes = self._request("api/v1/notes")
                logging.info(f"Retrieved {len(notes)} total notes")

                for note in notes:
                    flattened = flatten_json(note)
                    all_notes.append(flattened)

            except requests.exceptions.RequestException as e:
                logging.error(f"Error retrieving notes: {e}")

        else:
            # Get all available result dates
            logging.info("Fetching available result dates...")
            result_dates = self.get_results_dates()
            logging.info(f"Found {len(result_dates)} result dates")

            # Filter dates by since_date
            result_dates = [rd for rd in result_dates if rd["date"] > since_date]
            logging.info(f"Filtered to {len(result_dates)} dates after {since_date}")

            # Fetch notes for each date
            for idx, result_date in enumerate(result_dates, 1):
                date = result_date["date"]

                logging.info(f"Processing date {idx}/{len(result_dates)}: {date}")

                try:
                    # Get notes for this date from API
                    notes = self._request(f"api/v1/notes?date={date}")
                    logging.info(f"  Retrieved {len(notes)} notes for {date}")

                    # Flatten each note
                    for note in notes:
                        flattened = flatten_json(note)
                        all_notes.append(flattened)

                except requests.exceptions.Timeout:
                    logging.error(f"  Timeout retrieving notes for {date}, skipping...")
                    continue
                except requests.exceptions.RequestException as e:
                    logging.error(f"  Error retrieving notes for {date}: {e}, skipping...")
                    continue

        logging.info(f"Completed: retrieved {len(all_notes)} total notes")
        return all_notes

    def get_general_risks(self, since_date: str = None) -> list[dict]:
        """Get all general risks from Friday Pulse, optionally filtered by date.

        This function fetches general risk data from the API.
        Risks are flattened for easy database insertion.

        Args:
            since_date: Optional date string (YYYY-MM-DD) to filter risks created after this date.
                       If None, fetches risks without date filtering from the API.

        Returns:
            List of flattened dictionaries containing risk data, ready for database insertion
        """
        all_risks = []

        # If no since_date provided, fetch all risks without date parameter
        if since_date is None:
            logging.info("Fetching all general risks without date filter...")
            try:
                risks = self._request("api/v1/risk")
                logging.info(f"Retrieved {len(risks)} total general risks")

                for risk in risks:
                    flattened = flatten_json(risk)
                    all_risks.append(flattened)

            except requests.exceptions.RequestException as e:
                logging.error(f"Error retrieving general risks: {e}")

        else:
            # Get all available result dates
            logging.info("Fetching available result dates...")
            result_dates = self.get_results_dates()
            logging.info(f"Found {len(result_dates)} result dates")

            # Filter dates by since_date
            result_dates = [rd for rd in result_dates if rd["date"] > since_date]
            logging.info(f"Filtered to {len(result_dates)} dates after {since_date}")

            # Fetch risks for each date
            for idx, result_date in enumerate(result_dates, 1):
                date = result_date["date"]

                logging.info(f"Processing date {idx}/{len(result_dates)}: {date}")

                try:
                    # Get risks for this date from API
                    risks = self._request(f"api/v1/risk?date={date}")
                    logging.info(f"  Retrieved {len(risks)} general risks for {date}")

                    # Flatten each risk
                    for risk in risks:
                        flattened = flatten_json(risk)
                        all_risks.append(flattened)

                except requests.exceptions.Timeout:
                    logging.error(f"  Timeout retrieving general risks for {date}, skipping...")
                    continue
                except requests.exceptions.RequestException as e:
                    logging.error(f"  Error retrieving general risks for {date}: {e}, skipping...")
                    continue

        logging.info(f"Completed: retrieved {len(all_risks)} total general risks")
        return all_risks

    def get_group_risks(self, since_date: str = None) -> list[dict]:
        """Get risks for all groups from Friday Pulse, optionally filtered by date.

        This function fetches all available result dates, then retrieves risks for each
        group for each date. Risks are flattened for easy database insertion.

        Args:
            since_date: Optional date string (YYYY-MM-DD) to filter risks created after this date

        Returns:
            List of flattened dictionaries containing group risk data, ready for database insertion
        """
        all_risks = []

        # Get all groups
        logging.info("Fetching all groups for risks...")
        all_groups = self.get_groups()
        group_ids = [g.get("id") for g in all_groups if g.get("id")]
        logging.info(f"Found {len(group_ids)} groups total")

        # If no since_date provided, fetch latest risks without date parameter
        if since_date is None:
            logging.info("Fetching latest risks for all groups without date filter...")

            for group_idx, group_id in enumerate(group_ids, 1):
                try:
                    # Get latest risks for this group (no date parameter)
                    url = f"api/v1/groups/{group_id}/risk"
                    items = self._request(url)

                    if items:
                        logging.debug(f"  Group {group_idx}/{len(group_ids)} ({group_id}): {len(items)} risks")

                    # Flatten each item and add group_id
                    for item in items:
                        flattened = flatten_json(item)
                        flattened["group_id"] = group_id
                        all_risks.append(flattened)

                except requests.exceptions.Timeout:
                    logging.warning(f"  Timeout retrieving risks for group {group_id}, skipping...")
                    continue
                except requests.exceptions.RequestException as e:
                    logging.warning(f"  Error retrieving risks for group {group_id}: {e}, skipping...")
                    continue

            logging.info(f"Completed: retrieved {len(all_risks)} total risks from {len(group_ids)} groups")

        else:
            # Get and filter result dates
            logging.info("Fetching available result dates...")
            result_dates = self.get_results_dates()
            logging.info(f"Found {len(result_dates)} result dates")
            result_dates = self._filter_dates_by_since(result_dates, since_date)

            # Fetch risks for each group for each date
            all_risks = self._fetch_group_data_by_dates(
                group_ids, result_dates, "api/v1/groups/{group_id}/risk?date={date}", "risks"
            )

            logging.info(
                f"Completed: retrieved {len(all_risks)} total risks from "
                f"{len(group_ids)} groups across {len(result_dates)} dates"
            )

        return all_risks

    def get_group_notes(self, since_date: str = None) -> list[dict]:
        """Get notes for all groups from Friday Pulse, optionally filtered by date.

        This function fetches all available result dates, then retrieves notes for each
        group for each date. Notes are flattened for easy database insertion.

        Args:
            since_date: Optional date string (YYYY-MM-DD) to filter notes created after this date

        Returns:
            List of flattened dictionaries containing group notes data, ready for database insertion
        """
        all_notes = []

        # Get all groups
        logging.info("Fetching all groups for notes...")
        all_groups = self.get_groups()
        group_ids = [g.get("id") for g in all_groups if g.get("id")]
        logging.info(f"Found {len(group_ids)} groups total")

        # If no since_date provided, fetch latest notes without date parameter
        if since_date is None:
            logging.info("Fetching latest notes for all groups without date filter...")

            for group_idx, group_id in enumerate(group_ids, 1):
                try:
                    # Get latest notes for this group (no date parameter)
                    url = f"api/v1/groups/{group_id}/notes"
                    items = self._request(url)

                    if items:
                        logging.debug(f"  Group {group_idx}/{len(group_ids)} ({group_id}): {len(items)} notes")

                    # Flatten each item and add group_id
                    for item in items:
                        flattened = flatten_json(item)
                        flattened["group_id"] = group_id
                        all_notes.append(flattened)

                except requests.exceptions.Timeout:
                    logging.warning(f"  Timeout retrieving notes for group {group_id}, skipping...")
                    continue
                except requests.exceptions.RequestException as e:
                    logging.warning(f"  Error retrieving notes for group {group_id}: {e}, skipping...")
                    continue

            logging.info(f"Completed: retrieved {len(all_notes)} total notes from {len(group_ids)} groups")

        else:
            # Get and filter result dates
            logging.info("Fetching available result dates...")
            result_dates = self.get_results_dates()
            logging.info(f"Found {len(result_dates)} result dates")
            result_dates = self._filter_dates_by_since(result_dates, since_date)

            # Fetch notes for each group for each date
            all_notes = self._fetch_group_data_by_dates(
                group_ids, result_dates, "api/v1/groups/{group_id}/notes?date={date}", "notes"
            )

            logging.info(
                f"Completed: retrieved {len(all_notes)} total notes from "
                f"{len(group_ids)} groups across {len(result_dates)} dates"
            )

        return all_notes
