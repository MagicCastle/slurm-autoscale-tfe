"""Module providing the class to interact with Terraform Cloud API"""

import json

from collections import namedtuple
from datetime import datetime, UTC

import requests
from requests.structures import CaseInsensitiveDict
from requests.exceptions import HTTPError

WORKSPACE_API = "https://app.terraform.io/api/v2/workspaces"
RUNS_API = "https://app.terraform.io/api/v2/runs"
API_CONTENT = "application/vnd.api+json"

WorkspaceLock = namedtuple("WorkspaceLock", ["locked", "type", "id", "last_update"])

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

    def get(self, url):
        resp = requests.get(url, headers=self.headers, timeout=self.timeout)
        if not resp.ok:
            raise HTTPError(
                f"TFE API returned error code {resp.status_code}: {resp.reason}"
            )
        return resp

    def patch(self, url, **kargs):
        resp = requests.patch(
            url, headers=self.headers, timeout=self.timeout, **kargs,
        )
        if not resp.ok:
            raise HTTPError(
                f"TFE API returned error code {resp.status_code}: {resp.reason}"
            )
        return resp

    def post(self, url, **kargs):
        resp = requests.post(
            url, headers=self.headers, timeout=self.timeout, **kargs
        )
        if not resp.ok:
            raise HTTPError(
                f"TFE API returned error code {resp.status_code}: {resp.reason}"
            )
        return resp

    def get_workspace_lock(self):
        """Return a WorkspaceLock named tuple with the workspace lock state
        (locked, type, id).
        """
        url = "/".join((WORKSPACE_API, self.workspace))
        resp = self.get(url)
        data = resp.json()["data"]
        if data["attributes"]["locked"]:
            lock_type = data["relationships"]["locked-by"]["data"]["type"]
            lock_id = data["relationships"]["locked-by"]["data"]["id"]
            last_update = datetime.strptime(
                data["attributes"]["updated-at"], '%Y-%m-%dT%H:%M:%S.%fZ'
            ).replace(tzinfo=UTC)
            return WorkspaceLock(locked=True, type=lock_type, id=lock_id, last_update=last_update)
        return WorkspaceLock(locked=False, type=None, id=None, last_update=None)

    def fetch_variable(self, var_name):
        """Get a workspace variable content"""
        url = "/".join((WORKSPACE_API, self.workspace, "vars"))
        resp = self.get(url)
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
            resp = self.get(url)
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
        resp = self.patch(url, json=patch_data)
        return resp

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
        resp = self.post(RUNS_API, json=run_data)
        return resp.json()["data"]["id"]

    def get_run_status(self, run_id):
        """Return status of run"""
        url = "/".join((RUNS_API, run_id))
        resp = self.get(url)
        return resp.json()["data"]["attributes"]["status"]
