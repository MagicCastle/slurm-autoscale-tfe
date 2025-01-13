"""Module providing the class to interact with Terraform Cloud API
"""

import json

import requests
from requests.structures import CaseInsensitiveDict

WORKSPACE_API = "https://app.terraform.io/api/v2/workspaces"
RUNS_API = "https://app.terraform.io/api/v2/runs"
API_CONTENT = "application/vnd.api+json"


class InvalidAPIToken(Exception):
    """Raised when the TFE API token is invalid"""


class InvalidWorkspaceId(Exception):
    """Raised when the TFE workspace ID is invalid"""


class TFECLient:
    """TFEClient provides functions to:
    - retrieve a Terraform Cloud variable content
    - update a Terraform cloud variable content
    - queue a run
    """

    def __init__(self, token, workspace, timeout=5):
        self.token = token
        self.workspace = workspace
        self.headers = CaseInsensitiveDict()
        self.headers["Accept"] = API_CONTENT
        self.headers["Content-Type"] = API_CONTENT
        self.headers["Authorization"] = f"Bearer {token}"
        self.timeout = timeout

        # Validate init parameters by trying to retrieve workspace
        url = "/".join((WORKSPACE_API, self.workspace))
        resp = requests.get(url, headers=self.headers, timeout=self.timeout).json()
        if "errors" in resp:
            if resp["errors"][0]["status"] == "401":
                raise InvalidAPIToken
            if resp["errors"][0]["status"] == "404":
                raise InvalidWorkspaceId

    def fetch_variable(self, var_name):
        """Get a workspace variable content"""
        url = "/".join((WORKSPACE_API, self.workspace, "vars"))
        resp = requests.get(url, headers=self.headers, timeout=self.timeout)
        data = resp.json()["data"]
        for var in data:
            if var["attributes"]["key"] == var_name:
                return {
                    "id": var["id"],
                    "value": json.loads(var["attributes"]["value"]),
                }
        return None

    def fetch_resources(self):
        """Get all resources from the workspace"""
        url = "/".join((WORKSPACE_API, self.workspace, "resources"))
        resources = []
        while url is not None:
            resp = requests.get(url, headers=self.headers, timeout=self.timeout)
            json_ = resp.json()
            data = json_["data"]
            resources.extend(data)
            url = json_["links"]["next"]
        return resources

    def update_variable(self, var_id, value):
        """Update a workspace variable content"""
        patch_data = {
            "data": {
                "id": var_id,
                "attributes": {
                    "value": json.dumps(value),
                    "hcl": True,
                    "category": "terraform",
                },
            }
        }
        url = "/".join((WORKSPACE_API, self.workspace, "vars", var_id))
        return requests.patch(
            url, headers=self.headers, json=patch_data, timeout=self.timeout
        )

    def apply(self, message, targets):
        """Queue a workspace run"""
        run_data = {
            "data": {
                "attributes": {
                    "message": message,
                    "target-addrs": targets,
                    "auto-apply": True,
                },
                "relationships": {
                    "workspace": {"data": {"type": "workspaces", "id": self.workspace}},
                },
            }
        }
        resp = requests.post(
            RUNS_API, headers=self.headers, json=run_data, timeout=self.timeout
        )
        return resp.json()["data"]["id"]

    def get_run_status(self, run_id):
        """Return status of run"""
        url = "/".join((RUNS_API, run_id))
        resp = requests.get(url, headers=self.headers, timeout=self.timeout)
        return resp.json()["data"]["attributes"]["status"]
