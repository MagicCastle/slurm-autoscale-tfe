#!/usr/bin/env python3
"""Main module providing Slurm autoscaling functions with Terraform Cloud
"""
import logging
import sys

from enum import Enum
from os import environ
from subprocess import run, PIPE
from requests.exceptions import Timeout

from hostlist import expand_hostlist

from .tfe import TFECLient, InvalidAPIToken, InvalidWorkspaceId

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

POOL_VAR = environ.get("TFE_POOL_VAR", "pool")


class AutoscaleException(Exception):
    """Raised when something bad happened in autoscale main"""


class Commands(Enum):
    """Enumerate the name of script's commands"""
    RESUME = "resume"
    SUSPEND = "suspend"


def resume(hostlist=sys.argv[-1]):
    """Issue a request to Terraform cloud to power up the instances listed in
    hostlist.
    """
    try:
        main(Commands.RESUME, frozenset.union, hostlist)
    except AutoscaleException as exc:
        logging.error(str(exc))
        return 1
    return 0


def suspend(hostlist=sys.argv[-1]):
    """Issue a request to Terraform cloud to power down the instances listed in
    hostlist.
    """
    try:
        main(Commands.SUSPEND, frozenset.difference, hostlist)
    except AutoscaleException as exc:
        logging.error(str(exc))
        return 1
    return 0


def main(command, set_op, hostlist):
    """Issue a request to Terraform cloud to modify the pool variable of the
    workspace indicated by TFE_WORKSPACE environment variable using the operation
    provided as set_op and the hostnames provided in hostlist.
    """
    if environ.get("TFE_TOKEN", "") == "":
        raise AutoscaleException(
            f"{sys.argv[0]} requires environment variable TFE_TOKEN"
        )
    if environ.get("TFE_WORKSPACE", "") == "":
        raise AutoscaleException(
            f"{sys.argv[0]} requires environment variable TFE_WORKSPACE"
        )

    try:
        tfe_client = TFECLient(
            token=environ["TFE_TOKEN"],
            workspace=environ["TFE_WORKSPACE"],
        )
    except InvalidAPIToken as exc:
        raise AutoscaleException("invalid TFE API token") from exc
    except InvalidWorkspaceId as exc:
        raise AutoscaleException("invalid TFE workspace id") from exc
    except Timeout as exc:
        raise AutoscaleException("Connection to Terraform cloud timeout (5s)") from exc

    hosts = expand_hostlist(hostlist)
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
        tfe_pool = frozenset(tfe_var["value"])
    else:
        tfe_pool = frozenset()

    # Verify that TFE pool corresponds to Slurm pool:
    # When a powered up node fail to respond after slurm.conf's ResumeTimeout
    # slurmctld marks the node as "DOWN", but it will not call the SuspendProgram
    # on the node. Therefore, a change drift can happen between Slurm internal memory
    # of what nodes are online and the Terraform Cloud pool variable. To limit the
    # drift effect, we validate the state in Slurm of each node present in Terraform Cloud
    # pool variable. We only keep the nodes that are present in Slurm.
    try:
        scontrol_run = run(
            ["scontrol", "show", "-o", "node", ",".join(tfe_pool)],
            stdout=PIPE,
            stderr=PIPE,
            check=False,
        )
    except FileNotFoundError as exc:
        raise AutoscaleException("Cannot find command scontrol") from exc
    if scontrol_run.stderr:
        raise AutoscaleException(
            f"Error while calling scontrol {scontrol_run.stderr.decode()}"
        )

    scontrol_lines = scontrol_run.stdout.decode().split("\n")
    slurm_pool = frozenset(
        (
            node
            for node, line in zip(tfe_pool, scontrol_lines)
            if line.startswith(f"NodeName={node}")
        )
    )

    new_pool = set_op(slurm_pool, hosts)

    if tfe_pool != new_pool:
        try:
            tfe_client.update_variable(tfe_var["id"], list(new_pool))
        except Timeout as exc:
            raise AutoscaleException("Connection to Terraform cloud timeout (5s)") from exc
    else:
        logging.info(
            'TFE pool was already correctly set when "%s %s" was issued', command.value, hostlist,
        )

    try:
        tfe_client.apply(f"Slurm {command.value} {hostlist}")
    except Timeout as exc:
        raise AutoscaleException("Connection to Terraform cloud timeout (5s)") from exc


if __name__ == "__main__":
    if sys.argv[1] == Commands.RESUME.value:
        sys.exit(resume())
    elif sys.argv[1] == Commands.SUSPEND.value:
        sys.exit(suspend())
