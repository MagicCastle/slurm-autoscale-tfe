#!/usr/bin/env python3
import enum
import json
import os
import sys

from hostlist import expand_hostlist
import requests
from requests.structures import CaseInsensitiveDict


TF_WORKSPACE_API = "https://app.terraform.io/api/v2/workspaces"
TF_RUNS_API = "https://app.terraform.io/api/v2/runs"
TF_API_CONTENT = "application/vnd.api+json"

TOKEN = os.environ["TF_CLOUD_TOKEN"]
WORKSPACE = os.environ["TF_CLOUD_WORKSPACE"]
VAR_NAME = os.environ["TF_CLOUD_VAR_NAME"]

HEADERS = CaseInsensitiveDict()
HEADERS["Accept"] = TF_API_CONTENT
HEADERS["Content-Type"] = TF_API_CONTENT
HEADERS["Authorization"] = f"Bearer {TOKEN}"


class Commands(enum.Enum):
    RESUME = "resume"
    SUSPEND = "suspend"


def fetch_variable(var_name):
    url = "/".join((TF_WORKSPACE_API, WORKSPACE, "vars"))
    resp = requests.get(url, headers=HEADERS)
    data = resp.json()["data"]
    for var in data:
        if var["attributes"]["key"] == var_name:
            return var["id"], json.loads(var["attributes"]["value"])

    return None, None


def update_variable(var_id, hosts):
    patch_data = {
        "data": {
            "id": var_id,
            "attributes": {
                "value": json.dumps(list(hosts)),
                "hcl": True,
                "category": "terraform",
            },
        }
    }
    url = "/".join((TF_WORKSPACE_API, WORKSPACE, "vars", var_id))
    return requests.patch(url, headers=HEADERS, json=patch_data)


def apply(workspace, message):
    run_data = {
        "data": {
            "attributes": {"message": message},
            "relationships": {
                "workspace": {"data": {"type": "workspaces", "id": workspace}},
            },
        }
    }
    return requests.post(TF_RUNS_API, headers=HEADERS, json=run_data)


def main():
    if len(sys.argv) == 3:
        command = Commands(sys.argv[1])
    elif len(sys.argv) == 2:
        bin_name = os.path.basename(sys.argv[0])
        command = Commands(bin_name.split("_", 1)[-1])
    else:
        print("Usage: slurm_elastic [resume,suspend] <hostlist>")
        sys.exit(1)

    hostlist = sys.argv[-1]
    hosts = expand_hostlist(hostlist)
    var_id, old_hosts = fetch_variable(VAR_NAME)
    if var_id is None:
        print("Variable not found")
        sys.exit(1)

    new_hosts = set(old_hosts)
    if command == Commands.RESUME:
        new_hosts.update(hosts)
    elif command == Commands.SUSPEND:
        new_hosts.difference_update(hosts)

    if frozenset(old_hosts) != new_hosts:
        update_variable(var_id, new_hosts)
        apply(WORKSPACE, f"Slurm {command.value} {hostlist}")
    else:
        print("No change")


if __name__ == "__main__":
    main()
