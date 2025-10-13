"""
Wrapper for Friday Pulse API integration.

This module integrates the Friday Pulse client code directly and provides
functions to fetch and flatten results data for warehouse loading.
"""

import logging
from typing import Any

import requests


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

    def results_dates(self) -> list[dict]:
        """Retrieve the available results dates from the API.

        Returns:
            List of dictionaries with 'date' and 'question_count' keys
        """
        response = self._request("api/v1/info/results-dates")
        return response

    def results(self, date: str) -> list[dict]:
        """Retrieve results for a given date from the API.

        Args:
            date: Date string in format 'YYYY-MM-DD'

        Returns:
            List of result dictionaries with sample_date, score, response_rate,
            response_count, total_count, and topic fields
        """
        response = self._request(f"api/v1/results?date={date}")
        return response


def get_all_results(bearer_token: str, since_date: str = None, max_dates: int = None) -> list[dict]:
    """Get all results from Friday Pulse, optionally filtered by date.

    This function fetches all available result dates and retrieves results for each date.
    Results are flattened for easy database insertion.

    Args:
        bearer_token: The bearer token for API authentication
        since_date: Optional date string (YYYY-MM-DD) to filter results created after this date
        max_dates: Optional maximum number of dates to process (useful for testing or incremental loads)

    Returns:
        List of flattened dictionaries containing result data, ready for database insertion
    """
    client = FridayPulseClient(bearer_token)

    # Get all available result dates
    logging.info("Fetching available result dates...")
    result_dates = client.results_dates()
    logging.info(f"Found {len(result_dates)} result dates")

    # Filter dates if since_date is provided
    if since_date:
        result_dates = [rd for rd in result_dates if rd["date"] > since_date]
        logging.info(f"Filtered to {len(result_dates)} dates after {since_date}")

    # Limit number of dates if specified
    if max_dates:
        result_dates = result_dates[:max_dates]
        logging.info(f"Limited to {len(result_dates)} dates (max_dates={max_dates})")

    # Fetch results for each date
    all_results = []
    for idx, result_date in enumerate(result_dates, 1):
        date = result_date["date"]
        question_count = result_date["question_count"]

        logging.info(
            f"Processing date {idx}/{len(result_dates)}: {date} ({question_count} questions)"
        )

        try:
            # Get results for this date
            results = client.results(date)
            logging.info(f"  Retrieved {len(results)} results for {date}")

            # Flatten each result and add question_count
            for result in results:
                flattened = _flatten_result(result, question_count)
                all_results.append(flattened)

        except requests.exceptions.Timeout:
            logging.error(f"  Timeout retrieving results for {date}, skipping...")
            continue
        except requests.exceptions.RequestException as e:
            logging.error(f"  Error retrieving results for {date}: {e}, skipping...")
            continue

    logging.info(
        f"Completed: retrieved {len(all_results)} total results from {len(result_dates)} dates"
    )
    return all_results


def _flatten_result(result: dict, question_count: int) -> dict:
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
