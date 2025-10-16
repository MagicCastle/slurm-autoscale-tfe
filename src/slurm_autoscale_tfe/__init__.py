#!/usr/bin/env python3
"""Main module providing Slurm autoscaling functions with Terraform Cloud"""
import logging
import re
import sys
import time
import json

from enum import Enum
from os import environ
from subprocess import run, PIPE
from requests.exceptions import Timeout, HTTPError

from filelock import FileLock
from hostlist import collect_hostlist, expand_hostlist

from .tfe import TFECLient, InvalidAPIToken, InvalidWorkspaceId

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

SLEEP_TIME = 10
POOL_VAR = environ.get("TFE_POOL_VAR", "pool")

NODE_STATE_REGEX = re.compile(r"^NodeName=([a-z0-9-]*).*State=([A-Z_+]*).*$")
DOWN_FLAG_SET = frozenset(["DOWN", "POWER_DOWN", "POWERED_DOWN", "POWERING_DOWN"])
INSTANCE_TYPES = frozenset(
    [
        "aws_instance",
        "azurerm_linux_virtual_machine",
        "google_compute_instance",
        "openstack_compute_instance_v2",
        "incus_instance",
    ]
)


class AutoscaleException(Exception):
    """Raised when something bad happened in autoscale main"""


class Commands(Enum):
    """Enumerate the name of script's commands"""

    RESUME = "resume"
    RESUME_FAIL = "resume_fail"
    SUSPEND = "suspend"


def change_host_state(hostlist, state, reason=None):
    """Change the state of the hostlist in Slurm with scontrol.
    Called when an exception occured and we have to revert course with
    the state set by Slurm after calling resumeprogram or suspendprogram.
    """
    reason = [f"reason={reason}"] if reason is not None else []
    try:
        scontrol_run = run(
            ["scontrol", "update", f"NodeName={hostlist}", f"state={state}"] + reason,
            stdout=PIPE,
            stderr=PIPE,
            check=False,
        )
    except FileNotFoundError as exc:
        raise AutoscaleException("Cannot find command scontrol") from exc
    if scontrol_run.stderr:
        raise AutoscaleException(
            f"Error while calling scontrol update: {scontrol_run.stderr.decode()}"
        )


def list_nodes_with_states(states):
    try:
        scontrol_run = run(
            ["scontrol", "show", "node", "--json"],
            stdout=PIPE,
            stderr=PIPE,
            check=False,
        )
    except FileNotFoundError as exc:
        raise AutoscaleException("Cannot find command scontrol") from exc
    if scontrol_run.stderr:
        raise AutoscaleException(
            f"Error while calling scontrol update: {scontrol_run.stderr.decode()}"
        )
    all_nodes = json.loads(scontrol_run.stdout.decode())
    states_nodes = set(
        node["hostname"]
        for node in all_nodes["nodes"]
        if all(state in node["state"] for state in states)
    )
    return states_nodes


def create_maint_resv(hostlist, duration="5:00"):
    """Create a maintenance reservation starting now and lasting {duration}
    on the provided list of nodes.
    """
    try:
        scontrol_run = run(
            [
                "scontrol",
                "create",
                "reservation",
                "StartTime=now",
                "Flags=MAINT",
                f"Nodes={hostlist}",
                f"Duration={duration}",
                "User=root",
            ],
            stdout=PIPE,
            stderr=PIPE,
            check=False,
        )
    except FileNotFoundError as exc:
        raise AutoscaleException("Cannot find command scontrol") from exc
    if scontrol_run.stderr:
        raise AutoscaleException(
            f"Error while calling scontrol update: {scontrol_run.stderr.decode()}"
        )


def suspend_cloud_scaling(hostlist=None, duration="5:00"):
    nodes = set()
    if hostlist is not None:
        nodes.update(expand_hostlist(hostlist))
    nodes.update(list_nodes_with_states(("POWERED_DOWN", "CLOUD")))
    resv_hostlist = collect_hostlist(nodes)
    create_maint_resv(resv_hostlist, duration=duration)


def resume(hostlist=sys.argv[-1]):
    """Issue a request to Terraform cloud to power up the instances listed in
    hostlist.
    """
    try:
        main(Commands.RESUME, frozenset.union, hostlist)
    except AutoscaleException as exc:
        logging.error("Failed to resume '%s': %s", hostlist, str(exc))
        suspend_cloud_scaling(hostlist)
        return 1
    return 0


def suspend(hostlist=sys.argv[-1]):
    """Issue a request to Terraform cloud to power down the instances listed in
    hostlist.
    """
    try:
        main(Commands.SUSPEND, frozenset.difference, hostlist)
    except AutoscaleException as exc:
        logging.error("Failed to suspend '%s': %s", hostlist, str(exc))
        suspend_cloud_scaling(hostlist)
        return 1
    return 0


def resume_fail(hostlist=sys.argv[-1]):
    """Issue a request to Terraform cloud to power down the instances listed in
    hostlist.
    """
    try:
        main(Commands.RESUME_FAIL, frozenset.difference, hostlist)
    except AutoscaleException as exc:
        logging.error("Failed to resume_fail '%s': %s", hostlist, str(exc))
        suspend_cloud_scaling(hostlist)
        return 1
    return 0


def connect_tfe_client():
    """Return a TFE client object using environment variables for authentication"""
    if "TFE_TOKEN" not in environ:
        raise AutoscaleException(
            f"{sys.argv[0]} requires environment variable TFE_TOKEN"
        )
    if "TFE_WORKSPACE" not in environ:
        raise AutoscaleException(
            f"{sys.argv[0]} requires environment variable TFE_WORKSPACE"
        )

    try:
        return TFECLient(
            token=environ["TFE_TOKEN"],
            workspace=environ["TFE_WORKSPACE"],
        )
    except InvalidAPIToken as exc:
        raise AutoscaleException("invalid TFE API token") from exc
    except InvalidWorkspaceId as exc:
        raise AutoscaleException("invalid TFE workspace id") from exc
    except Timeout as exc:
        raise AutoscaleException("Connection to Terraform cloud timeout (5s)") from exc


def get_pool_from_tfe(tfe_client):
    """Retrieve id and content of POOL variable from Terraform cloud"""
    try:
        tfe_var = tfe_client.fetch_variable(POOL_VAR)
    except Timeout as exc:
        raise AutoscaleException("Connection to Terraform cloud timeout (5s)") from exc

    if tfe_var is None:
        raise AutoscaleException(
            f'"{POOL_VAR}" variable not found in TFE workspace "{environ["TFE_WORKSPACE"]}"'
        )

    # When the pool variable was incorrectly initialized in the workspace,
    # we avoid a catastrophe by setting the initial pool as an empty set.
    if isinstance(tfe_var["value"], list):
        return tfe_var["id"], frozenset(tfe_var["value"])
    return tfe_var["id"], frozenset()


def get_instances_from_tfe(tfe_resources, hosts):
    """Return resource addresses from Terraform cloud that match hosts list."""
    instances = []
    for resource in tfe_resources:
        if (
            resource["attributes"]["provider-type"] in INSTANCE_TYPES
            and resource["attributes"]["name-index"] in hosts
        ):
            instances.append(f"module.{resource['attributes']['address']}")
    return frozenset(instances)


def get_provisioners_from_tfe(tfe_resources):
    """Return the provisioner resource address"""
    provisioners = []
    for resource in tfe_resources:
        if resource["attributes"]["provider-type"] == "terraform_data":
            address = resource["attributes"]["address"].split(".")
            address = f"module.{address[0]}.module.{address[1]}.{'.'.join(address[2:])}"
            provisioners.append(address)
    return frozenset(provisioners)


def wait_on_workspace_lock(tfe_client, max_run_time=60):
    """Wait up to 60 seconds per unique run for the workspace to unlock.
    If the workspace is locked by a user or something else, throw an
    AutoscaleException as there is no way of telling when the lock might
    be lifted.
    """
    workspace_lock_count = 0
    lock_run_id = None
    while True:
        try:
            workspace_lock = tfe_client.get_workspace_lock()
        except HTTPError as exc:
            raise AutoscaleException(
                "Could not retrieve workspace lock status, giving up scaling."
            ) from exc
        if not workspace_lock.locked:
            return
        if (
            workspace_lock_count * SLEEP_TIME >= max_run_time
            and lock_run_id == workspace_lock.id
        ):
            raise AutoscaleException(
                f"TFE workspace has been locked for "
                f"{max_run_time}s by {lock_run_id}, giving up scaling."
            )
        if workspace_lock.type == "runs":
            if lock_run_id != workspace_lock.id:
                lock_run_id = workspace_lock.id
                workspace_lock_count = 0
            else:
                workspace_lock_count += 1
            time.sleep(SLEEP_TIME)
        else:
            raise AutoscaleException(
                f"TFE {workspace_lock.id} locked the workspace, cannot scale."
            )


def main(command, set_op, hostlist):
    """Issue a request to Terraform cloud to modify the pool variable of the
    workspace indicated by TFE_WORKSPACE environment variable using the operation
    provided as set_op and the hostnames provided in hostlist.
    """
    hosts = frozenset(expand_hostlist(hostlist))
    tfe_client = connect_tfe_client()
    with FileLock("/tmp/slurm_autoscale_tfe_pool.lock"):
        wait_on_workspace_lock(tfe_client, max_run_time=60)
        var_id, tfe_pool = get_pool_from_tfe(tfe_client)
        next_pool = set_op(tfe_pool, hosts)
        if tfe_pool != next_pool:
            try:
                tfe_client.update_variable(var_id, list(next_pool))
            except HTTPError as exc:
                raise AutoscaleException(
                    "TFE API returned an error code when trying to update the pool variable"
                ) from exc
            except Timeout as exc:
                raise AutoscaleException(
                    "Connection to Terraform cloud timeout (5s)"
                ) from exc
        else:
            logging.warning(
                'TFE pool variable is unchanged following the issue of "%s %s"',
                command.value,
                hostlist,
            )

    try:
        tfe_resources = tfe_client.fetch_resources()
    except HTTPError as exc:
        raise AutoscaleException(
            "TFE API returned an error code when trying to fetch the resources"
        ) from exc
    except Timeout as exc:
        raise AutoscaleException("Connection to Terraform cloud timeout (5s)") from exc

    instances = get_instances_from_tfe(tfe_resources, hosts)
    provisioners = get_provisioners_from_tfe(tfe_resources)
    try:
        run_id = tfe_client.apply(
            f"Slurm {command.value} {hostlist}".strip(),
            targets=list(instances | provisioners),
        )
    except HTTPError as exc:
        raise AutoscaleException(
            "TFE API returned an error code when trying to submit the run"
        ) from exc
    except Timeout as exc:
        raise AutoscaleException("Connection to Terraform cloud timeout (5s)") from exc
    logging.info("%s %s (%s)", command.value, hostlist, run_id)

    if command == Commands.RESUME_FAIL:
        change_host_state(hostlist, "IDLE")
