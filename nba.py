import json
import os
import time
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

load_dotenv()


class NBA:
    def __init__(self) -> None:
        self.url = "https://v2.nba.api-sports.io"
        self.api_key = os.getenv("API_KEY")
        assert self.api_key
        self.headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": "v2.nba.api-sports.io",
        }
        self.timeout = 90

    def fetch(self, endpoint) -> List[Dict[str, Any]]:
        response = requests.get(
            url=self.url + endpoint, headers=self.headers, timeout=self.timeout
        )
        data = response.json()
        # print(json.dumps(data, indent=2))
        if isinstance(data.get("errors", {}), (Dict, dict)) and "rateLimit" in data.get(
            "errors", {}
        ):
            print(
                "Retry in 60 seconds due to error:\n"
                + json.dumps(data["errors"], indent=2)
            )
            time.sleep(60)
            return self.fetch(endpoint)
        return data["response"]

    def fetch_teams(self) -> List[Dict[str, Any]]:
        endpoint = "/teams"
        return self.fetch(endpoint)

    def fetch_seasons(self) -> List[int]:
        endpoint = "/seasons"
        return self.fetch(endpoint)

    def fetch_games(self, season: str) -> List[Dict[str, Any]]:
        endpoint = f"/games?season={season}"
        return self.fetch(endpoint)
