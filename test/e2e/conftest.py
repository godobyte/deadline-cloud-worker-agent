# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3
import dataclasses
import logging
import os
import pytest
from dataclasses import dataclass, field, InitVar
from typing import Callable, Generator, Type
from contextlib import contextmanager

from deadline_test_fixtures import (
    DeadlineWorker,
    DeadlineWorkerConfiguration,
    DockerContainerWorker,
    Farm,
    Fleet,
    Queue,
    EC2InstanceWorker,
    BootstrapResources,
    PosixSessionUser,
    OperatingSystem,
)
import pytest

LOG = logging.getLogger(__name__)

pytest_plugins = ["deadline_test_fixtures.pytest_hooks"]


@dataclass(frozen=True)
class DeadlineResources:
    farm: Farm = field(init=False)
    queue_a: Queue = field(init=False)
    queue_b: Queue = field(init=False)
    non_valid_role_queue: Queue = field(init=False)
    fleet: Fleet = field(init=False)
    scaling_queue: Queue = field(init=False)
    scaling_fleet: Fleet = field(init=False)

    farm_id: InitVar[str]
    queue_a_id: InitVar[str]
    queue_b_id: InitVar[str]
    non_valid_role_queue_id: InitVar[str]
    fleet_id: InitVar[str]
    scaling_queue_id: InitVar[str]
    scaling_fleet_id: InitVar[str]

    def __post_init__(
        self,
        farm_id: str,
        queue_a_id: str,
        queue_b_id: str,
        non_valid_role_queue_id: str,
        fleet_id: str,
        scaling_queue_id: str,
        scaling_fleet_id: str,
    ) -> None:
        object.__setattr__(self, "farm", Farm(id=farm_id))
        object.__setattr__(self, "queue_a", Queue(id=queue_a_id, farm=self.farm))
        object.__setattr__(self, "queue_b", Queue(id=queue_b_id, farm=self.farm))
        object.__setattr__(
            self, "non_valid_role_queue", Queue(id=non_valid_role_queue_id, farm=self.farm)
        )
        object.__setattr__(self, "fleet", Fleet(id=fleet_id, farm=self.farm, autoscaling=False))
        object.__setattr__(self, "scaling_queue", Queue(id=scaling_queue_id, farm=self.farm))
        object.__setattr__(self, "scaling_fleet", Fleet(id=scaling_fleet_id, farm=self.farm))


@pytest.fixture(scope="session")
def deadline_resources() -> Generator[DeadlineResources, None, None]:
    """
    Gets Deadline resources required for running tests.

    Environment Variables:
        FARM_ID: ID of the Deadline farm to use.
        QUEUE_A_ID: ID of a non scaling Deadline queue to use for tests.
        QUEUE_B_ID: ID of a non scaling Deadline queue to use for tests.
        NON_VALID_ROLE_QUEUE_ID: ID of a non scaling Deadline queue with a role that cannot read the S3 bucket to use for tests
        FLEET_ID: ID of a non scaling Deadline fleet to use for tests.
        SCALING_QUEUE_ID: ID of the Deadline scaling queue to use.
        SCALING_FLEET_ID: ID of the Deadline scaling fleet to use.

    Returns:
        DeadlineResources: The Deadline resources used for tests
    """
    farm_id = os.environ["FARM_ID"]
    queue_a_id = os.environ["QUEUE_A_ID"]
    queue_b_id = os.environ["QUEUE_B_ID"]
    non_valid_role_queue_id = os.environ["NON_VALID_ROLE_QUEUE_ID"]
    fleet_id = os.environ["FLEET_ID"]

    scaling_queue_id = os.environ["SCALING_QUEUE_ID"]
    scaling_fleet_id = os.environ["SCALING_FLEET_ID"]

    LOG.info(
        f"Configured Deadline Cloud Resources, farm: {farm_id}, scaling_fleet: {scaling_fleet_id}, scaling_queue: {scaling_queue_id} ,queue_a: {queue_a_id}, queue_b: {queue_b_id}, fleet: {fleet_id}"
    )

    sts_client = boto3.client("sts")
    response = sts_client.get_caller_identity()
    LOG.info("Running tests with credentials from: %s" % response.get("Arn"))

    yield DeadlineResources(
        farm_id=farm_id,
        queue_a_id=queue_a_id,
        queue_b_id=queue_b_id,
        non_valid_role_queue_id=non_valid_role_queue_id,
        fleet_id=fleet_id,
        scaling_queue_id=scaling_queue_id,
        scaling_fleet_id=scaling_fleet_id,
    )


@pytest.fixture(scope="session")
def worker_config(
    posix_job_user: PosixSessionUser,
    posix_env_override_job_user: PosixSessionUser,
    posix_config_override_job_user: PosixSessionUser,
    worker_config: DeadlineWorkerConfiguration,
    windows_job_users: list[str],
) -> DeadlineWorkerConfiguration:
    """
    Builds the configuration for a DeadlineWorker.

    Environment Variables:
        WORKER_POSIX_USER: The POSIX user to configure the worker for
            Defaults to "deadline-worker"
        WORKER_POSIX_SHARED_GROUP: The shared POSIX group to configure the worker user and job user with
            Defaults to "shared-group"
        WORKER_AGENT_WHL_PATH: Path to the Worker agent wheel file to use.
        WORKER_AGENT_REQUIREMENT_SPECIFIER: PEP 508 requirement specifier for the Worker agent package.
            If WORKER_AGENT_WHL_PATH is provided, this option is ignored.
        LOCAL_MODEL_PATH: Path to a local Deadline model file to use for API calls.
            If DEADLINE_SERVICE_MODEL_S3_URI was provided, this option is ignored.

    Returns:
        DeadlineWorkerConfiguration: Configuration for use by DeadlineWorker.
    """
    return dataclasses.replace(
        worker_config,
        job_users=[posix_job_user, posix_config_override_job_user, posix_env_override_job_user],
        windows_job_users=windows_job_users,
    )


@pytest.fixture(scope="session")
def session_worker(
    request: pytest.FixtureRequest,
    worker_config: DeadlineWorkerConfiguration,
    ec2_worker_type: Type[EC2InstanceWorker],
) -> Generator[DeadlineWorker, None, None]:
    with create_worker(worker_config, ec2_worker_type, request) as worker:
        yield worker

    stop_worker(request, worker)


@pytest.fixture(scope="class")
def class_worker(
    request: pytest.FixtureRequest,
    worker_config: DeadlineWorkerConfiguration,
    ec2_worker_type: Type[EC2InstanceWorker],
) -> Generator[DeadlineWorker, None, None]:
    with create_worker(worker_config, ec2_worker_type, request) as worker:
        yield worker

    stop_worker(request, worker)


@pytest.fixture(scope="function")
def function_worker(
    request: pytest.FixtureRequest,
    worker_config: DeadlineWorkerConfiguration,
    ec2_worker_type: Type[EC2InstanceWorker],
) -> Generator[DeadlineWorker, None, None]:
    with create_worker(worker_config, ec2_worker_type, request) as worker:
        yield worker

    stop_worker(request, worker)


@pytest.fixture(scope="function")
def function_worker_factory(
    request: pytest.FixtureRequest,
    ec2_worker_type: Type[EC2InstanceWorker],
) -> Generator[Callable[[DeadlineWorkerConfiguration], EC2InstanceWorker], None, None]:

    created_workers = []

    def _create_function_worker(
        custom_worker_config: DeadlineWorkerConfiguration,
    ):
        with create_worker(custom_worker_config, ec2_worker_type, request) as worker:
            created_workers.append(worker)
            return worker

    yield _create_function_worker
    for worker in created_workers:
        stop_worker(request, worker)


def create_worker(
    worker_config: DeadlineWorkerConfiguration,
    ec2_worker_type: Type[EC2InstanceWorker],
    request: pytest.FixtureRequest,
):
    def __init__(self):
        pass

    def __enter_(self):
        print("Entering the context")

    def __exit__(self, exc_type, exc_value, traceback):
        print("Exiting the context")

    """
    Gets a DeadlineWorker for use in tests.

    Environment Variables:
        SUBNET_ID: The subnet ID to deploy the EC2 worker into.
            This is required for EC2 workers. Does not apply if USE_DOCKER_WORKER is true.
        SECURITY_GROUP_ID: The security group ID to deploy the EC2 worker into.
            This is required for EC2 workers. Does not apply if USE_DOCKER_WORKER is true.
        AMI_ID: The AMI ID to use for the Worker agent.
            Defaults to the latest AL2023 AMI.
            Does not apply if USE_DOCKER_WORKER is true.
        USE_DOCKER_WORKER: If set to "true", this fixture will create a Worker that runs in a local Docker container instead of an EC2 instance.
        KEEP_WORKER_AFTER_FAILURE: If set to "true", will not destroy the Worker when it fails. Useful for debugging. Default is "false"

    Returns:
        DeadlineWorker: Instance of the DeadlineWorker class that can be used to interact with the Worker.
    """

    worker: DeadlineWorker
    if os.environ.get("USE_DOCKER_WORKER", "").lower() == "true":
        LOG.info("Creating Docker worker")
        worker = DockerContainerWorker(
            configuration=worker_config,
        )
    else:
        LOG.info("Creating EC2 worker")
        ami_id = os.getenv("AMI_ID")
        subnet_id = os.getenv("SUBNET_ID")
        security_group_id = os.getenv("SECURITY_GROUP_ID")
        instance_type = os.getenv("WORKER_INSTANCE_TYPE", default="t3.large")
        instance_shutdown_behavior = os.getenv("WORKER_INSTANCE_SHUTDOWN_BEHAVIOR", default="stop")

        assert subnet_id, "SUBNET_ID is required when deploying an EC2 worker"
        assert security_group_id, "SECURITY_GROUP_ID is required when deploying an EC2 worker"

        bootstrap_resources: BootstrapResources = request.getfixturevalue("bootstrap_resources")
        assert (
            bootstrap_resources.worker_instance_profile_name
        ), "Worker instance profile is required when deploying an EC2 worker"

        ec2_client = boto3.client("ec2")
        s3_client = boto3.client("s3")
        ssm_client = boto3.client("ssm")
        deadline_client = boto3.client("deadline")

        worker = ec2_worker_type(
            ec2_client=ec2_client,
            s3_client=s3_client,
            deadline_client=deadline_client,
            bootstrap_bucket_name=bootstrap_resources.bootstrap_bucket_name,
            ssm_client=ssm_client,
            override_ami_id=ami_id,
            subnet_id=subnet_id,
            security_group_id=security_group_id,
            instance_profile_name=bootstrap_resources.worker_instance_profile_name,
            configuration=worker_config,
            instance_type=instance_type,
            instance_shutdown_behavior=instance_shutdown_behavior,
        )

    @contextmanager
    def _context_for_fixture():
        try:
            worker.start()
        except Exception as e:
            LOG.exception(f"Failed to start worker: {e}")
            LOG.info("Stopping worker because it failed to start")
            stop_worker(request, worker)
            raise
        yield worker

    return _context_for_fixture()


def stop_worker(request: pytest.FixtureRequest, worker: DeadlineWorker) -> None:
    if request.session.testsfailed > 0:
        if os.getenv("KEEP_WORKER_AFTER_FAILURE", "false").lower() == "true":
            LOG.info("KEEP_WORKER_AFTER_FAILURE is set, not stopping worker")
            return

    try:
        worker.stop()
    except Exception as e:
        LOG.exception(f"Error while stopping worker: {e}")
        LOG.error(
            "Failed to stop worker. Resources may be left over that need to be cleaned up manually."
        )
        raise


@pytest.fixture(scope="session")
def region() -> str:
    return os.getenv("REGION", os.getenv("AWS_DEFAULT_REGION", "us-west-2"))


@pytest.fixture(scope="session")
def posix_job_user() -> PosixSessionUser:
    return PosixSessionUser(
        user="job-user",
        group="job-user",
    )


@pytest.fixture(scope="session")
def posix_config_override_job_user() -> PosixSessionUser:
    return PosixSessionUser(
        user="config-override",
        group="job-override-group",
    )


@pytest.fixture(scope="session")
def posix_env_override_job_user() -> PosixSessionUser:
    return PosixSessionUser(
        user="env-override",
        group="job-override-group",
    )


@pytest.fixture(scope="session")
def generic_non_queue_job_user() -> PosixSessionUser:
    return PosixSessionUser(
        user="non-queue-user",
        group="job-override-group",
    )


@pytest.fixture(scope="session")
def windows_job_users() -> list[str]:
    return [
        "job-user",
        "cli-override",
        "config-override",
        "install-override",
        "env-override",
    ]


@pytest.fixture(scope="session")
def operating_system() -> OperatingSystem:
    os_env_var = os.environ.get("OPERATING_SYSTEM")
    if os_env_var == "linux":
        return OperatingSystem(name="AL2023")
    elif os_env_var == "windows":
        return OperatingSystem(name="WIN2022")
    else:
        assert (
            False
        ), f'Expected OPERATING_SYSTEM env var to be "linux" or "windows", but got {os_env_var}'


def pytest_collection_modifyitems(items):
    sorted_list = list(items)
    for item in items:
        # Run session scoped tests last to prevent Worker conflicts with class and function scoped tests.
        if "session_worker" in item.fixturenames:
            sorted_list.remove(item)
            sorted_list.append(item)

    items[:] = sorted_list
