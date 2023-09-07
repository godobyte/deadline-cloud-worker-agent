# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from argparse import ArgumentParser, Namespace
from pathlib import Path


class ParsedCommandLineArguments(Namespace):
    """Represents the parsed Amazon Deadline Cloud Worker Agent command-line arguments"""

    farm_id: str | None = None
    fleet_id: str | None = None
    cleanup_session_user_processes: bool | None = None
    profile: str | None = None
    verbose: bool | None = None
    no_shutdown: bool | None = None
    impersonation: bool | None = None
    posix_job_user: str | None = None
    allow_instance_profile: bool | None = None
    logs_dir: Path | None = None
    local_session_logs: bool | None = None
    persistence_dir: Path | None = None
    # TODO: Remove when deprecating --no-allow-instance-profile
    no_allow_instance_profile: bool | None = None


def get_argument_parser() -> ArgumentParser:
    """Returns a command-line argument parser for the Amazon Deadline Cloud Worker Agent"""
    parser = ArgumentParser(
        prog="deadline-worker-agent", description="Amazon Deadline Cloud Worker Agent"
    )
    parser.add_argument(
        "--farm-id",
        help="The Amazon Deadline Cloud Farm identifier that the Worker should register to",
        default=None,
    )
    parser.add_argument(
        "--fleet-id",
        help="The Amazon Deadline Cloud Fleet identifier that the Worker should register to",
        default=None,
    )
    parser.add_argument(
        "--no-cleanup-session-user-processes",
        help="Whether to cleanup leftover processes running as a session user when that user is no longer being used in any active session",
        dest="cleanup_session_user_processes",
        action="store_const",
        const=False,
        default=None,
    )
    parser.add_argument(
        "--profile",
        help="The AWS profile to use",
        default=None,
    )
    parser.add_argument(
        "--no-shutdown",
        help="Does not shutdown the instance during scale-in event.",
        action="store_const",
        const=True,
        default=None,
    )
    parser.add_argument(
        "--no-impersonation",
        help="Does not use OS impersonation to run actions. WARNING: this is insecure - for development use only.",
        action="store_const",
        const=False,
        dest="impersonation",
        default=None,
    )
    parser.add_argument(
        "--posix-job-user",
        help="Overrides the posix user that the Worker Agent impersonates. Format: 'user:group'. "
        "If not set, defaults to what the service sets.",
        default=None,
    )
    parser.add_argument(
        "--logs-dir",
        help="Overrides the directory where the Worker Agent writes its logs.",
        default=None,
        type=Path,
    )
    parser.add_argument(
        "--no-local-session-logs",
        help="Turns off writing of session logs to the local filesystem",
        dest="local_session_logs",
        action="store_const",
        const=False,
        default=None,
    )
    parser.add_argument(
        "--persistence-dir",
        help="Overrides the directory where the Worker Agent persists files across restarts.",
        default=None,
        type=Path,
    )
    # TODO: This is deprecated. Remove this eventually
    parser.add_argument(
        "--no-allow-instance-profile",
        help="DEPRECATED. This does nothing",
        action="store_true",
        dest="no_allow_instance_profile",
    )
    parser.add_argument(
        "--allow-instance-profile",
        help="Turns off validation that the host EC2 instance profile is disassociated before starting",
        action="store_const",
        const=True,
        dest="allow_instance_profile",
        default=None,
    )
    parser.add_argument(
        "--verbose",
        "-v",
        help="Use verbose console logging",
        action="store_const",
        const=True,
        default=None,
    )
    return parser