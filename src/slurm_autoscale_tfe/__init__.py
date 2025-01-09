#!/usr/bin/env python3
"""Main module providing Slurm autoscaling functions with Terraform Cloud
"""
import logging
import re
import sys

from enum import Enum
from os import environ
from subprocess import run, PIPE
from requests.exceptions import Timeout

from filelock import FileLock
from hostlist import expand_hostlist

from .tfe import TFECLient, InvalidAPIToken, InvalidWorkspaceId

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

POOL_VAR = environ.get("TFE_POOL_VAR", "pool")

NODE_STATE_REGEX = re.compile(r"^NodeName=([a-z0-9-]*).*State=([A-Z_+]*).*$")
DOWN_FLAG_SET = frozenset(["DOWN", "POWER_DOWN", "POWERED_DOWN", "POWERING_DOWN"])
INSTANCE_TYPES = frozenset(
    [
        "aws_instance",
        "azurerm_linux_virtual_machine",
        "google_compute_instance",
        "openstack_compute_instance_v2",
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


def resume(hostlist=sys.argv[-1]):
    """Issue a request to Terraform cloud to power up the instances listed in
    hostlist.
    """
    try:
        main(Commands.RESUME, frozenset.union, hostlist)
    except AutoscaleException as exc:
        logging.error("Failed to resume '%s': %s", hostlist, str(exc))
        change_host_state(hostlist, "DOWN", reason=str(exc))
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
        change_host_state(hostlist, "DOWN", reason=str(exc))
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
        change_host_state(hostlist, "DOWN", reason=str(exc))
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


def get_instances_from_tfe(tfe_client):
    """Return all names of instances that are created in Terraform Cloud state."""
    try:
        tfe_resources = tfe_client.fetch_resources()
    except Timeout as exc:
        raise AutoscaleException("Connection to Terraform cloud timeout (5s)") from exc
    instances = []
    address_prefix = None
    for resource in tfe_resources:
        if resource["attributes"]["provider-type"] in INSTANCE_TYPES:
            instances.append(resource["attributes"]["name-index"])
            address_prefix = resource["attributes"]["address"].split("[")[0]
    return frozenset(instances), address_prefix


def main(command, set_op, hostlist):
    """Issue a request to Terraform cloud to modify the pool variable of the
    workspace indicated by TFE_WORKSPACE environment variable using the operation
    provided as set_op and the hostnames provided in hostlist.
    """
    hosts = frozenset(expand_hostlist(hostlist))
    tfe_client = connect_tfe_client()

    with FileLock("/tmp/slurm_autoscale_tfe_pool.lock"):
        var_id, tfe_pool = get_pool_from_tfe(tfe_client)
        next_pool = set_op(tfe_pool, hosts)
        if tfe_pool != next_pool:
            try:
                tfe_client.update_variable(var_id, list(next_pool))
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

    _, address_prefix = get_instances_from_tfe(tfe_client)
    try:
        run_id = tfe_client.apply(
            f"Slurm {command.value} {hostlist}".strip(),
            targets=[f'module.{address_prefix}["{hostname}"]' for hostname in hosts],
        )
    except Timeout as exc:
        raise AutoscaleException("Connection to Terraform cloud timeout (5s)") from exc
    logging.info("%s %s (%s)", command.value, hostlist, run_id)

    if command == Commands.RESUME_FAIL:
        change_host_state(hostlist, "IDLE")
