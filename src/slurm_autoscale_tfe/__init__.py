#!/usr/bin/env python3
import logging

from enum import Enum
from os import environ
from sys import argv, exit
from subprocess import getoutput

from hostlist import expand_hostlist

from .tfe import TFECLient, InvalidAPIToken, InvalidWorkspaceId

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

POOL_VAR = environ.get("TFE_POOL_VAR", "pool")

class AutoscaleException(Exception):
    "Raised when something bad happened in autoscale main"
    pass

class Commands(Enum):
    RESUME = "resume"
    SUSPEND = "suspend"

def resume(hostlist=argv[-1]):
    main(Commands.RESUME, frozenset.union, hostlist)

def suspend(hostlist=argv[-1]):
    main(Commands.SUSPEND, frozenset.difference, hostlist)

def main(command, op, hostlist):
    if environ.get("TFE_TOKEN", "") == "":
        raise AutoscaleException("{} requires environment variable TFE_TOKEN".format(argv[0]))
    if environ.get("TFE_WORKSPACE", "") == "":
        raise AutoscaleException("{} requires environment variable TFE_WORKSPACE".format(argv[0]))

    try:
        tfe_client = TFECLient(
            token=environ["TFE_TOKEN"],
            workspace=environ["TFE_WORKSPACE"],
        )
    except InvalidAPIToken:
        raise AutoscaleException("invalid TFE API token")
    except InvalidWorkspaceId:
        raise AutoscaleException("invalid TFE workspace id")

    hosts = expand_hostlist(hostlist)
    tfe_var = tfe_client.fetch_variable(POOL_VAR)
    if tfe_var is None:
        raise AutoscaleException(f'"{POOL_VAR}" variable not found in TFE workspace "{environ["TFE_WORKSPACE"]}"')

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
    scontrol_lines = getoutput(f"scontrol show -o node {','.join(tfe_pool)}").split('\n')
    slurm_pool = frozenset((
        node for node, line in zip(tfe_pool, scontrol_lines)
        if line.startswith(f"NodeName={node}")
    ))

    new_pool = op(slurm_pool, hosts)

    if tfe_pool != new_pool:
        tfe_client.update_variable(tfe_var["id"], list(new_pool))
        tfe_client.apply(f"Slurm {command.value} {hostlist}")
    else:
        logging.info(f"No change found while trying to {command} node {hostlist}")


if __name__ == "__main__":
    try:
        if argv[1] == Commands.RESUME.value:
            resume()
        elif argv[1] == Commands.SUSPEND.value:
            suspend()
    except AutoscaleException as e:
        logging.error(str(e))
        exit(1)
    else:
        exit(0)
