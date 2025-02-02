# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from concurrent.futures import (
    Executor,
)
import os
import sys
from pathlib import Path
from logging import LoggerAdapter
from typing import Any, TYPE_CHECKING, Optional
from dataclasses import asdict

from deadline.job_attachments.asset_manifests import BaseAssetManifest
from deadline.job_attachments.models import (
    Attachments,
    PathFormat,
    JobAttachmentS3Settings,
    ManifestProperties,
    PathMappingRule,
    JobAttachmentsFileSystem,
)
from deadline.job_attachments.os_file_permission import (
    FileSystemPermissionSettings,
    PosixFileSystemPermissionSettings,
    WindowsFileSystemPermissionSettings,
    WindowsPermissionEnum,
)

from openjd.sessions import (
    LOG as OPENJD_LOG,
    LogContent,
    PathMappingRule as OpenjdPathMapping,
    PosixSessionUser,
    WindowsSessionUser,
)
from openjd.model.v2023_09 import (
    EmbeddedFileTypes as EmbeddedFileTypes_2023_09,
    EmbeddedFileText as EmbeddedFileText_2023_09,
    Action as Action_2023_09,
    StepScript as StepScript_2023_09,
    StepActions as StepActions_2023_09,
)
from openjd.model import ParameterValue

from ...log_messages import SessionActionLogKind
from .openjd_action import OpenjdAction

if TYPE_CHECKING:
    from ..session import Session
    from ..job_entities import JobAttachmentDetails, StepDetails


class AttachmentDownloadAction(OpenjdAction):
    """Action to synchronize input job attachments for a AWS Deadline Cloud Session

    Parameters
    ----------
    id : str
        The unique action identifier
    """

    _job_attachment_details: Optional[JobAttachmentDetails]
    _step_details: Optional[StepDetails]
    _step_script: Optional[StepScript_2023_09]

    def __init__(
        self,
        *,
        id: str,
        session_id: str,
        job_attachment_details: Optional[JobAttachmentDetails] = None,
        step_details: Optional[StepDetails] = None,
    ) -> None:
        super(AttachmentDownloadAction, self).__init__(
            id=id,
            action_log_kind=(
                SessionActionLogKind.JA_SYNC_INPUT
                if step_details is None
                else SessionActionLogKind.JA_DEP_SYNC
            ),
            step_id=step_details.step_id if step_details is not None else None,
        )
        self._job_attachment_details = job_attachment_details
        self._step_details = step_details
        self._logger = LoggerAdapter(OPENJD_LOG, extra={"session_id": session_id})

    def set_step_script(self, manifests: list[str], s3_settings: JobAttachmentS3Settings) -> None:
        """Sets the step script for the action

        Parameters
        ----------
        manifests : list[str]
            The job attachment manifest paths
        s3_settings : JobAttachmentS3Settings
            The job attachment S3 settings
        """
        args = [
            "{{ Task.File.AttachmentDownload }}",
            "-pm",
            "{{ Session.PathMappingRulesFile }}",
            "-s3",
            s3_settings.to_s3_root_uri(),
            "-m",
        ]
        args.extend(manifests)

        executable_path = Path(sys.executable)
        python_path = executable_path.parent / executable_path.name.lower().replace(
            "pythonservice.exe", "python.exe"
        )

        with open(Path(__file__).parent / "scripts" / "attachment_download.py", "r") as f:
            self._step_script = StepScript_2023_09(
                actions=StepActions_2023_09(
                    onRun=Action_2023_09(
                        command=str(python_path),
                        args=args,
                    )
                ),
                embeddedFiles=[
                    EmbeddedFileText_2023_09(
                        name="AttachmentDownload",
                        filename="download.py",
                        type=EmbeddedFileTypes_2023_09.TEXT,
                        data=f.read(),
                    )
                ],
            )

    def __eq__(self, other: Any) -> bool:
        return (
            type(self) is type(other)
            and self._id == other._id
            and self._job_attachment_details == other._job_attachment_details
            and self._step_details == other._step_details
        )

    def start(
        self,
        *,
        session: Session,
        executor: Executor,
    ) -> None:
        """Initiates the synchronization of the input job attachments

        Parameters
        ----------
        session : Session
            The Session that is the target of the action
        executor : Executor
            An executor for running futures
        """

        if self._step_details:
            section_title = "Job Attachments Download for Step"
        else:
            section_title = "Job Attachments Download for Job"

        # Banner mimicing the one printed by the openjd-sessions runtime
        # TODO - Consider a better approach to manage the banner title
        self._logger.info(
            "==============================================",
            extra={"openjd_log_content": LogContent.BANNER},
        )
        self._logger.info(
            f"--------- AttachmentDownloadAction  {section_title}",
            extra={"openjd_log_content": LogContent.BANNER},
        )
        self._logger.info(
            "==============================================",
            extra={"openjd_log_content": LogContent.BANNER},
        )

        if not (job_attachment_settings := session._job_details.job_attachment_settings):
            raise RuntimeError("Job attachment settings were not contained in JOB_DETAILS entity")

        if self._job_attachment_details:
            session._job_attachment_details = self._job_attachment_details

        # Validate that job attachment details have been provided before syncing step dependencies.
        if session._job_attachment_details is None:
            raise RuntimeError(
                "Job attachments must be synchronized before downloading Step dependencies."
            )

        step_dependencies = self._step_details.dependencies if self._step_details else []

        assert job_attachment_settings.s3_bucket_name is not None
        assert job_attachment_settings.root_prefix is not None
        assert session._asset_sync is not None

        s3_settings = JobAttachmentS3Settings(
            s3BucketName=job_attachment_settings.s3_bucket_name,
            rootPrefix=job_attachment_settings.root_prefix,
        )

        manifest_properties_list: list[ManifestProperties] = []
        if not step_dependencies:
            for manifest_properties in session._job_attachment_details.manifests:
                manifest_properties_list.append(
                    ManifestProperties(
                        rootPath=manifest_properties.root_path,
                        fileSystemLocationName=manifest_properties.file_system_location_name,
                        rootPathFormat=PathFormat(manifest_properties.root_path_format),
                        inputManifestPath=manifest_properties.input_manifest_path,
                        inputManifestHash=manifest_properties.input_manifest_hash,
                        outputRelativeDirectories=manifest_properties.output_relative_directories,
                    )
                )

        attachments = Attachments(
            manifests=manifest_properties_list,
            fileSystem=session._job_attachment_details.job_attachments_file_system,
        )

        storage_profiles_path_mapping_rules_dict: dict[str, str] = {
            str(rule.source_path): str(rule.destination_path)
            for rule in session._job_details.path_mapping_rules
        }

        # Generate absolute Path Mapping to local session (no storage profile)
        # returns root path to PathMappingRule mapping
        dynamic_mapping_rules: dict[str, PathMappingRule] = (
            session._asset_sync.generate_dynamic_path_mapping(
                session_dir=session.working_directory,
                attachments=attachments,
            )
        )

        # Aggregate manifests (with step step dependency handling)
        merged_manifests_by_root: dict[str, BaseAssetManifest] = (
            session._asset_sync._aggregate_asset_root_manifests(
                session_dir=session.working_directory,
                s3_settings=s3_settings,
                queue_id=session._queue_id,
                job_id=session._queue._job_id,
                attachments=attachments,
                step_dependencies=step_dependencies,
                dynamic_mapping_rules=dynamic_mapping_rules,
                storage_profiles_path_mapping_rules=storage_profiles_path_mapping_rules_dict,
            )
        )

        if self._start_vfs(
            session=session,
            attachments=attachments,
            merged_manifests_by_root=merged_manifests_by_root,
            s3_settings=s3_settings,
        ):
            # successfully launched VFS
            return

        job_attachment_path_mappings = list([asdict(r) for r in dynamic_mapping_rules.values()])

        # Open Job Description session implementation details -- path mappings are sorted.
        # bisect.insort only supports the 'key' arg in 3.10 or later, so
        # we first extend the list and sort it afterwards.
        if session.openjd_session._path_mapping_rules:
            session.openjd_session._path_mapping_rules.extend(
                OpenjdPathMapping.from_dict(r) for r in job_attachment_path_mappings
            )
        else:
            session.openjd_session._path_mapping_rules = [
                OpenjdPathMapping.from_dict(r) for r in job_attachment_path_mappings
            ]

        # Open Job Description Sessions sort the path mapping rules based on length of the parts make
        # rules that are subsets of each other behave in a predictable manner. We must
        # sort here since we're modifying that internal list appending to the list.
        session.openjd_session._path_mapping_rules.sort(
            key=lambda rule: -len(rule.source_path.parts)
        )

        manifest_paths_by_root = session._asset_sync._check_and_write_local_manifests(
            merged_manifests_by_root=merged_manifests_by_root,
            manifest_write_dir=str(session.working_directory),
        )
        session.manifest_paths_by_root = manifest_paths_by_root

        self.set_step_script(
            manifests=manifest_paths_by_root.values(),  # type: ignore
            s3_settings=s3_settings,
        )
        assert self._step_script is not None
        session.run_task(
            step_script=self._step_script,
            task_parameter_values=dict[str, ParameterValue](),
        )

    def _start_vfs(
        self,
        session: Session,
        attachments: Attachments,
        merged_manifests_by_root: dict[str, BaseAssetManifest],
        s3_settings: JobAttachmentS3Settings,
    ) -> bool:
        fs_permission_settings: Optional[FileSystemPermissionSettings] = None
        if session._os_user is not None:
            if os.name == "posix":
                if not isinstance(session._os_user, PosixSessionUser):
                    raise ValueError(f"The user must be a posix-user. Got {type(session._os_user)}")
                fs_permission_settings = PosixFileSystemPermissionSettings(
                    os_user=session._os_user.user,
                    os_group=session._os_user.group,
                    dir_mode=0o20,
                    file_mode=0o20,
                )
            else:
                if not isinstance(session._os_user, WindowsSessionUser):
                    raise ValueError(
                        f"The user must be a windows-user. Got {type(session._os_user)}"
                    )
                if session._os_user is not None:
                    fs_permission_settings = WindowsFileSystemPermissionSettings(
                        os_user=session._os_user.user,
                        dir_mode=WindowsPermissionEnum.WRITE,
                        file_mode=WindowsPermissionEnum.WRITE,
                    )

        if (
            attachments.fileSystem == JobAttachmentsFileSystem.VIRTUAL.value
            and sys.platform != "win32"
            and fs_permission_settings is not None
            and os.environ is not None
            and "AWS_PROFILE" in os.environ
            and isinstance(fs_permission_settings, PosixFileSystemPermissionSettings)
        ):
            assert session._asset_sync is not None
            session._asset_sync._launch_vfs(
                s3_settings=s3_settings,
                session_dir=session.working_directory,
                fs_permission_settings=fs_permission_settings,
                merged_manifests_by_root=merged_manifests_by_root,
                os_env_vars=dict(os.environ),
            )
            return True

        else:
            return False
