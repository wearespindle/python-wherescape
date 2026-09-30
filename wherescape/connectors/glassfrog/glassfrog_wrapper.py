"""
Glassfrog API v3 client.

Hits the meetings + circles endpoints with a Session-level retry adapter so
transient 429/5xx responses are handled without a manual retry loop.
"""

import logging
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://api.glassfrog.com/api/v3/"


class Glassfrog:
    def __init__(self, api_key: str):
        self._session = requests.Session()
        self._session.headers.update({"X-Auth-Token": api_key, "Accept": "application/json"})
        retry = Retry(
            total=5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            backoff_factor=10,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("https://", adapter)

    def _get(self, path: str, params: dict | None = None) -> dict | list:
        response = self._session.get(urljoin(BASE_URL, path), params=params, timeout=60)
        response.raise_for_status()
        return response.json()

    def _paginated(self, path: str, params: dict | None = None) -> list[dict]:
        """Walk page= until an empty page comes back.

        Glassfrog v3 wraps list responses under the resource key
        (e.g. ``{"governance_meetings": [...]}``); fall back to a bare list
        for endpoints that respond unwrapped.
        """
        params = dict(params or {})
        params.setdefault("per_page", 100)
        envelope_key = path.rsplit("/", 1)[-1]
        page = 1
        all_items: list[dict] = []
        while True:
            params["page"] = page
            body = self._get(path, params=params)
            if isinstance(body, dict):
                items = body.get(envelope_key, [])
            else:
                items = body
            if not items:
                break
            all_items.extend(items)
            logging.info(f"Fetched page {page} of {path}: {len(items)} items")
            page += 1
        return all_items

    def get_governance_meetings(self) -> list[dict]:
        return self._paginated("governance_meetings")

    def get_tactical_meetings(self) -> list[dict]:
        return self._paginated("tactical_meetings")

    def get_all_meetings(self) -> list[tuple[str, dict]]:
        """Return ``(meeting_type, meeting)`` tuples across every meeting endpoint.

        Adding a new endpoint later (e.g. strategy meetings) is a one-line change here.
        """
        return [("governance", m) for m in self.get_governance_meetings()] + [
            ("tactical", m) for m in self.get_tactical_meetings()
        ]

    def get_circles(self) -> list[dict]:
        return self._paginated("circles")
