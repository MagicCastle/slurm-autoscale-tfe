#!/usr/bin/env python3
import enum
import os
import sys

from hostlist import expand_hostlist

from .tfe import TFECLient

POOL_VAR = os.environ.get("TFE_POOL_VAR", "pool")

class Commands(enum.Enum):
    RESUME = "resume"
    SUSPEND = "suspend"

def resume(hostlist=sys.argv[-1]):
    main(Commands.RESUME, set.update, hostlist)

def suspend(hostlist=sys.argv[-1]):
    main(Commands.SUSPEND, set.intersection_update, hostlist)

def main(command, op, hostlist):
    tfe_client = TFECLient(
        token=os.environ["TFE_TOKEN"],
        workspace=os.environ["TFE_WORKSPACE"],
    )

    hosts = expand_hostlist(hostlist)
    pool = tfe_client.fetch_variable(POOL_VAR)
    if pool is None:
        print("\"{}\" variable not found in TFE workspace".format(POOL_VAR))
        sys.exit(1)

    cur_pool = frozenset(pool["value"])
    new_pool = set(cur_pool)
    op(new_pool, hosts)

    if new_pool != cur_pool:
        tfe_client.update_variable(pool["id"], list(new_pool))
        tfe_client.apply(f"Slurm {command.value} {hostlist}")
    else:
        print("No change")


if __name__ == "__main__":
    if sys.argv[1] == Commands.RESUME.value:
        resume()
    elif sys.argv[1] == Commands.SUSPEND.value:
        suspend()
