# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from typing import Optional, Tuple
from pathlib import Path

from pydantic import BaseSettings, Field
from pydantic.env_settings import SettingsSourceCallable

from .capabilities import Capabilities
from .config_file import ConfigFile


# Default path for the worker's logs.
DEFAULT_POSIX_WORKER_LOGS_DIR = Path("/var/log/amazon/deadline")
# Default path for the worker persistence directory.
# The persistence directory is expected to be located on a file-system that is local to the Worker
# Node. The Worker's ID and credentials are persisted and these should not be accessible by other
# Worker Nodes.
DEFAULT_POSIX_WORKER_PERSISTENCE_DIR = Path("/var/lib/deadline")


class WorkerSettings(BaseSettings):
    """Model class for the worker settings. This defines all of the fields and their validation as
    well as the settings sources and their priority order of:

    1. command-line arguments
    2. environment variables
    3. config file

    Parameters
    ----------
    farm_id : str
        The unique identifier of the worker's farm
    fleet_id: str
        The unique identifier of the worker's fleet
    cleanup_session_user_processes: bool
        Whether session user processes should be cleaned up when the session user is not being used
        in any active sessions anymore.
    profile : str
        An AWS profile used to bootstrap the worker
    verbose : bool
        Whether to emit more verbose logging
    no_shutdown : bool
        If true, then the Worker will not shut down when the service tells the worker to stop
    impersonation : bool
        Whether to use OS user impersonation (e.g. sudo on Linux) when running session actions
    posix_job_user : str
        Which 'user:group' to use instead of the Queue user when turned on.
    allow_instance_profile : bool
        If false (the default) and the worker is running on an EC2 instance with IMDS, then the
        worker will wait until the instance profile is disassociated before running worker sessions.
        This will repeatedly attempt to make requests to IMDS. If the instance profile is still
        associated after some threshold, the worker agent program will log the error and exit .
    capabilities : deadline_worker_agent.startup.Capabilities
        A set of capabilities that will be declared when the worker starts. These capabilities
        can be used by the service to determine if the worker is eligible to run sessions for a
        given job/step/task and whether the worker is compliant with its fleet's configured minimum
        capabilities.
    worker_logs_dir : Path
        The path to the directory where the Worker Agent writes its logs.
    worker_persistence_dir : Path
        The path to the directory where the Worker Agent persists its state.
    local_session_logs : bool
        Whether to write session logs to the local filesystem
    """

    farm_id: str = Field(regex=r"^farm-[a-z0-9]{32}$")
    fleet_id: str = Field(regex=r"^fleet-[a-z0-9]{32}$")
    cleanup_session_user_processes: bool = True
    profile: Optional[str] = Field(min_length=1, max_length=64, default=None)
    verbose: bool = False
    no_shutdown: bool = False
    impersonation: bool = True
    posix_job_user: Optional[str] = Field(
        regex=r"^[a-zA-Z0-9_.][^:]{0,31}:[a-zA-Z0-9_.][^:]{0,31}$"
    )
    allow_instance_profile: bool = False
    capabilities: Capabilities = Field(
        default_factory=lambda: Capabilities(amounts={}, attributes={})
    )
    worker_logs_dir: Path = DEFAULT_POSIX_WORKER_LOGS_DIR
    worker_persistence_dir: Path = DEFAULT_POSIX_WORKER_PERSISTENCE_DIR
    local_session_logs: bool = True

    class Config:
        fields = {
            "farm_id": {"env": "DEADLINE_WORKER_FARM_ID"},
            "fleet_id": {"env": "DEADLINE_WORKER_FLEET_ID"},
            "cleanup_session_user_processes": {
                "env": "DEADLINE_WORKER_CLEANUP_SESSION_USER_PROCESSES"
            },
            "profile": {"env": "DEADLINE_WORKER_PROFILE"},
            "verbose": {"env": "DEADLINE_WORKER_VERBOSE"},
            "no_shutdown": {"env": "DEADLINE_WORKER_NO_SHUTDOWN"},
            "impersonation": {"env": "DEADLINE_WORKER_IMPERSONATION"},
            "posix_job_user": {"env": "DEADLINE_WORKER_POSIX_JOB_USER"},
            "allow_instance_profile": {"env": "DEADLINE_WORKER_ALLOW_INSTANCE_PROFILE"},
            "capabilities": {"env": "DEADLINE_WORKER_CAPABILITIES"},
            "worker_logs_dir": {"env": "DEADLINE_WORKER_LOGS_DIR"},
            "worker_persistence_dir": {"env": "DEADLINE_WORKER_PERSISTENCE_DIR"},
            "local_session_logs": {"env": "DEADLINE_WORKER_LOCAL_SESSION_LOGS"},
        }

        @classmethod
        def customise_sources(
            cls,
            init_settings: SettingsSourceCallable,
            env_settings: SettingsSourceCallable,
            file_secret_settings: SettingsSourceCallable,
        ) -> Tuple[SettingsSourceCallable, ...]:
            """This function is called by pydantic to determine the settings sources used and their
            priority order.

            Below, we define the order as:

                1. Command-line arguments (passed in via the construct)
                2. Environment variables
                3. Configuration file

            Parameters
            ----------
            init_settings : pydantic.env_settings.SettingsSourceCallable
                The pydantic built-in init arguments settings source
            env_settings : pydantic.env_settings.SettingsSourceCallable
                The pydantic built-in environment variable setting ssource
            file_secret_settings : pydantic.env_settings.SettingsSourceCallable
                The pydantic built-in (Docker) secret file settings source

            Returns
            -------
            Tuple[pyadntic.env_settings.SettingsSourceCallable, ...]
                The settings sources used when initializing the WorkerSettings instance in priority
                order.
            """
            try:
                config_file = ConfigFile.load()
            except FileNotFoundError:
                return (init_settings, env_settings)

            return (
                init_settings,
                env_settings,
                config_file.as_settings,
            )