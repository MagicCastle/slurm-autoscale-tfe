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

from hostlist import expand_hostlist

from .tfe import TFECLient, InvalidAPIToken, InvalidWorkspaceId

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

POOL_VAR = environ.get("TFE_POOL_VAR", "pool")

NODE_STATE_REGEX = re.compile(r'^NodeName=([a-z0-9-]*).*State=([A-Z_+]*).*$')
DOWN_FLAG_SET = frozenset(['DOWN', 'POWER_DOWN', 'POWERED_DOWN', 'POWERING_DOWN'])

class AutoscaleException(Exception):
    """Raised when something bad happened in autoscale main"""


class Commands(Enum):
    """Enumerate the name of script's commands"""
    RESUME = "resume"
    SUSPEND = "suspend"


def change_host_state(hostlist, state, reason=None):
    """Change the state of the hostlist in Slurm with scontrol.
    Called when an exception occured and we have to revert course with
    the state set by Slurm after calling resumeprogram or suspendprogram.
    """
    reason = [f"reason={reason}"] if reason is not None else []
    run(
        ["scontrol", "update", f"NodeName={hostlist}", f"state={state}"] + reason,
        stdout=PIPE,
        stderr=PIPE,
        check=False,
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


def identify_online_nodes(tfe_pool):
    """Identify from a list of hosts which ones are online based on Slurm.
    """
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
    slurm_pool = []
    for line in scontrol_lines:
        match = NODE_STATE_REGEX.match(line)
        if match:
            node_state = frozenset(match.group(2).split('+'))
            if not node_state.intersection(DOWN_FLAG_SET):
                slurm_pool.append(match.group(1))

    return frozenset(slurm_pool)

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

    hosts = frozenset(expand_hostlist(hostlist))
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
    slurm_pool = identify_online_nodes(tfe_pool)
    zombie_nodes = tfe_pool - slurm_pool - hosts
    extra_command = ""
    if len(zombie_nodes) > 0:
        zombie_nodes_string = ",".join(sorted(zombie_nodes))
        logging.warning(
            'TFE vs Slurm drift detected, these nodes will be suspended: %s',
            zombie_nodes_string
        )
        extra_command = f" & suspend {zombie_nodes_string} (drift detection)"

    new_pool = set_op(slurm_pool, hosts)

    if tfe_pool != new_pool:
        try:
            tfe_client.update_variable(tfe_var["id"], list(new_pool))
        except Timeout as exc:
            raise AutoscaleException("Connection to Terraform cloud timeout (5s)") from exc
    else:
        logging.warning(
            'TFE pool was already correctly set when "%s %s" was issued', command.value, hostlist,
        )

    try:
        tfe_client.apply(f"Slurm {command.value} {hostlist} {extra_command}".strip())
    except Timeout as exc:
        raise AutoscaleException("Connection to Terraform cloud timeout (5s)") from exc
    logging.info("%s %s", command.value, hostlist)


if __name__ == "__main__":
    if sys.argv[1] == Commands.RESUME.value:
        sys.exit(resume())
    elif sys.argv[1] == Commands.SUSPEND.value:
        sys.exit(suspend())
