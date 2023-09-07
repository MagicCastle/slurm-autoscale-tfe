import json

import requests
from requests.structures import CaseInsensitiveDict

WORKSPACE_API = "https://app.terraform.io/api/v2/workspaces"
RUNS_API = "https://app.terraform.io/api/v2/runs"
API_CONTENT = "application/vnd.api+json"

class InvalidAPIToken(Exception):
    "Raised when the TFE API token is invalid"
    pass

class InvalidWorkspaceId(Exception):
    "Raised when the TFE workspace ID is invalid"
    pass

class TFECLient:
    def __init__(self, token, workspace):
        self.token = token
        self.workspace = workspace
        self.headers = CaseInsensitiveDict()
        self.headers["Accept"] = API_CONTENT
        self.headers["Content-Type"] = API_CONTENT
        self.headers["Authorization"] = "Bearer {}".format(token)

        # Validate init parameters by trying to retrieve workspace
        url = "/".join((WORKSPACE_API, self.workspace))
        resp = requests.get(url, headers=self.headers).json()
        if "errors" in resp:
            if resp["errors"][0]["status"] == '401':
                raise InvalidAPIToken
            elif resp["errors"][0]["status"] == '404':
                raise InvalidWorkspaceId

    def fetch_variable(self, var_name):
        url = "/".join((WORKSPACE_API, self.workspace, "vars"))
        resp = requests.get(url, headers=self.headers)
        data = resp.json()["data"]
        for var in data:
            if var["attributes"]["key"] == var_name:
                return {
                    "id": var["id"],
                    "value": json.loads(var["attributes"]["value"])
                }
        return None


    def update_variable(self, var_id, value):
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
        return requests.patch(url, headers=self.headers, json=patch_data)


    def apply(self, message):
        run_data = {
            "data": {
                "attributes": {"message": message},
                "relationships": {
                    "workspace": {"data": {"type": "workspaces", "id": self.workspace}},
                },
            }
        }
        return requests.post(RUNS_API, headers=self.headers, json=run_data)
