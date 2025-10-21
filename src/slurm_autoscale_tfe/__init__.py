#!/usr/bin/env python3
"""Main module providing Slurm autoscaling functions with Terraform Cloud"""
import json
import logging
import sys

from enum import Enum
from os import environ, path
from subprocess import run, PIPE, CalledProcessError
from datetime import datetime, timezone

from filelock import FileLock
from hostlist import expand_hostlist
from requests.exceptions import Timeout, HTTPError

from .tfe import TFEClient

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

POOL_VAR = environ.get("TFE_POOL_VAR", "pool")

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

def scontrol(arg_list):
    """Run Slurm scontrol command and return stdout as a string
    if the command could complete successfully.
    """
    try:
        scontrol_run = run(
            ["scontrol"] + arg_list,
            stdout=PIPE,
            stderr=PIPE,
            check=True,
        )
    except FileNotFoundError as exc:
        raise AutoscaleException("Cannot find command scontrol") from exc
    except CalledProcessError as exc:
        raise AutoscaleException(
            f"Error while calling scontrol: {exc}"
        ) from exc
    return scontrol_run.stdout.decode()

def change_host_state(hostlist, state, reason=None):
    """Change the state of the hostlist in Slurm with scontrol.
    Called when an exception occurred and we have to revert course with
    the state set by Slurm after calling resumeprogram or suspendprogram.
    """
    reason = [f"reason={reason}"] if reason is not None else []
    arg_list = ["update", f"NodeName={hostlist}", f"state={state}"] + reason
    return scontrol(arg_list)

def list_nodes_with_states(states):
    """Return a list of hostnames that present all {states} in their
    state list when listing their attributes with:
      scontrol show node --json <node_name>
    """
    arg_list = ["show", "node", "--json"]
    output = scontrol(arg_list)
    all_nodes = json.loads(output)
    states_nodes = set(
        node["hostname"]
        for node in all_nodes["nodes"]
        if all(state in node["state"] for state in states)
    )
    return states_nodes


def create_maint_resv(hostlist, comment, duration="5:00"):
    """Create a maintenance reservation starting now and lasting {duration}
    on the provided list of nodes.
    """
    arg_list = [
        "create",
        "reservation",
        "StartTime=now",
        "Flags=MAINT",
        f"Nodes={hostlist}",
        f"Duration={duration}",
        "User=root",
        f"Comment={comment}",
    ]
    return scontrol(arg_list)


def suspend_cloud_scaling(hostlist, comment, duration="5:00"):
    """Prevent Slurm from scheduling jobs on the nodes in {hostlist}
    for {duration}.

    If at some point we want to pause scheduling on all cloud nodes
    that are powered down, we would do the following
        nodes = set()
        if hostlist is not None:
        nodes.update(expand_hostlist(hostlist))
        nodes.update(list_nodes_with_states(("POWERED_DOWN", "CLOUD")))
        hostlist = collect_hostlist(nodes)
    """
    # To create a maintenance reservation of {duration}, the nodes
    # need to be not busy, so we change their state to DOWN first
    # so we can create the reservation. Once the reservation is
    # set, we can put them back in IDLE state, so job can be scheduled
    # once the maintenance {duration} is over.
    change_host_state(hostlist, state="DOWN", reason="suspend cloud scaling")
    create_maint_resv(hostlist, comment, duration=duration)
    change_host_state(hostlist, state="IDLE")


def resume(hostlist=sys.argv[-1]):
    """Issue a request to Terraform cloud to power up the instances listed in
    hostlist.
    """
    try:
        main(Commands.RESUME, frozenset.union, hostlist)
    except AutoscaleException as exc:
        msg = f"Failed to resume '{hostlist}': {exc}"
        logging.error(msg)
        change_host_state(hostlist, state="POWER_DOWN_FORCE", reason="failed to resume")
        suspend_cloud_scaling(hostlist, comment=msg)
        return 1
    return 0


def suspend(hostlist=sys.argv[-1]):
    """Issue a request to Terraform cloud to power down the instances listed in
    hostlist.
    """
    try:
        main(Commands.SUSPEND, frozenset.difference, hostlist)
    except AutoscaleException as exc:
        msg = f"Failed to suspend '{hostlist}': {exc}"
        logging.error(msg)
        suspend_cloud_scaling(hostlist, comment=msg)
        return 1
    return 0


def resume_fail(hostlist=sys.argv[-1]):
    """Issue a request to Terraform cloud to power down the instances listed in
    hostlist.
    """
    try:
        main(Commands.RESUME_FAIL, frozenset.difference, hostlist)
    except AutoscaleException as exc:
        msg = f"Failed to resume_fail '{hostlist}': {exc}"
        logging.error(msg)
        suspend_cloud_scaling(hostlist, comment=msg)
        return 1
    return 0


def create_tfe_client():
    """Return a TFE client object using environment variables for authentication"""
    if "TFE_TOKEN" not in environ:
        raise AutoscaleException(
            f"{sys.argv[0]} requires environment variable TFE_TOKEN"
        )
    if "TFE_WORKSPACE" not in environ:
        raise AutoscaleException(
            f"{sys.argv[0]} requires environment variable TFE_WORKSPACE"
        )

    return TFEClient(
        token=environ["TFE_TOKEN"],
        workspace=environ["TFE_WORKSPACE"],
    )


def get_pool_from_tfe(tfe_client):
    """Retrieve id and content of POOL variable from Terraform cloud"""
    try:
        tfe_var = tfe_client.fetch_variable(POOL_VAR)
    except Timeout as exc:
        raise AutoscaleException(
            f"Connection to Terraform cloud timeout ({tfe_client.timeout}s)"
        ) from exc

    if tfe_var is None:
        raise AutoscaleException(
            f'"{POOL_VAR}" variable not found in TFE workspace "{environ["TFE_WORKSPACE"]}"'
        )

    if isinstance(tfe_var["value"], list):
        return tfe_var["id"], frozenset(tfe_var["value"])

    # When the pool variable was incorrectly initialized in the workspace,
    # we avoid a catastrophe by ignoring the value and returning an empty set.
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


def check_workspace_lock(tfe_client, max_run_time=300):
    """Check if the workspace is currently locked by a user or if it has been
    locked by a single run for more than max_run_time. In which case, there is
    little hope of this run executing on time, so we give up.
    """
    try:
        workspace_lock = tfe_client.get_workspace_lock()
    except HTTPError as exc:
        raise AutoscaleException(
            f"Could not retrieve workspace lock status, giving up scaling. {exc}"
        ) from exc

    if not workspace_lock.locked:
        return

    if workspace_lock.type != "runs":
        raise AutoscaleException(
            f"TFE {workspace_lock.id} locked the workspace, cannot scale."
        )

    if (datetime.now(tz=timezone.utc) - workspace_lock.last_update).total_seconds() > max_run_time:
        raise AutoscaleException(
            f"TFE workspace has been locked for more than "
            f"{max_run_time}s by {workspace_lock.id}, giving up scaling."
        )


def get_slurmctld_state_location():
    """Return the value of StateSaveLocation from Slurm config
    using scontrol show config.
    """
    config_output = scontrol(["show", "config"])
    for line in config_output.split("\n"):
        try:
            lhs, rhs = line.split("=")
        except ValueError:
            continue
        if lhs.strip() == "StateSaveLocation":
            return rhs.strip()
    return "/var/spool"


def main(command, set_op, hostlist):
    """Issue a request to Terraform cloud to modify the pool variable of the
    workspace indicated by TFE_WORKSPACE environment variable using the operation
    provided as set_op and the hostnames provided in hostlist.
    """
    hosts = frozenset(expand_hostlist(hostlist))
    tfe_client = create_tfe_client()
    lock_path = path.join(get_slurmctld_state_location(), "autoscale_tfe_pool.lock")
    with FileLock(lock_path):
        check_workspace_lock(tfe_client, max_run_time=300)
        var_id, tfe_pool = get_pool_from_tfe(tfe_client)
        next_pool = set_op(tfe_pool, hosts)
        if tfe_pool != next_pool:
            try:
                tfe_client.update_variable(var_id, list(next_pool))
            except HTTPError as exc:
                raise AutoscaleException(
                    f"TFE API returned an error code when trying to update the pool variable. {exc}"
                ) from exc
            except Timeout as exc:
                raise AutoscaleException(
                    f"Connection to Terraform cloud timeout ({tfe_client.timeout}s)"
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
            f"TFE API returned an error code when trying to fetch the resources. {exc}"
        ) from exc
    except Timeout as exc:
        raise AutoscaleException(
            f"Connection to Terraform cloud timeout ({tfe_client.timeout}s)"
        ) from exc

    instances = get_instances_from_tfe(tfe_resources, hosts)
    provisioners = get_provisioners_from_tfe(tfe_resources)
    try:
        run_id = tfe_client.apply(
            f"Slurm {command.value} {hostlist}".strip(),
            targets=list(instances | provisioners),
        )
    except HTTPError as exc:
        raise AutoscaleException(
            f"TFE API returned an error code when trying to submit the run. {exc}"
        ) from exc
    except Timeout as exc:
        raise AutoscaleException(
            f"Connection to Terraform cloud timeout ({tfe_client.timeout}s)"
        ) from exc
    logging.info("%s %s (%s)", command.value, hostlist, run_id)

    if command == Commands.RESUME_FAIL:
        change_host_state(hostlist, "IDLE")
