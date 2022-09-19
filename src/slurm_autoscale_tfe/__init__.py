#!/usr/bin/env python3
from enum import Enum
from os import environ
from sys import argv

from hostlist import expand_hostlist

from .tfe import TFECLient

POOL_VAR = environ.get("TFE_POOL_VAR", "pool")

class Commands(Enum):
    RESUME = "resume"
    SUSPEND = "suspend"

def resume(hostlist=argv[-1]):
    main(Commands.RESUME, set.update, hostlist)

def suspend(hostlist=argv[-1]):
    main(Commands.SUSPEND, set.intersection_update, hostlist)

def main(command, op, hostlist):
    tfe_client = TFECLient(
        token=environ["TFE_TOKEN"],
        workspace=environ["TFE_WORKSPACE"],
    )

    hosts = expand_hostlist(hostlist)
    pool = tfe_client.fetch_variable(POOL_VAR)
    if pool is None:
        raise Exception('"{}" variable not found in TFE workspace'.format(POOL_VAR))

    cur_pool = frozenset(pool["value"])
    new_pool = set(cur_pool)
    op(new_pool, hosts)

    if new_pool != cur_pool:
        tfe_client.update_variable(pool["id"], list(new_pool))
        tfe_client.apply(f"Slurm {command.value} {hostlist}")
    else:
        print("No change")


if __name__ == "__main__":
    if argv[1] == Commands.RESUME.value:
        resume()
    elif argv[1] == Commands.SUSPEND.value:
        suspend()
