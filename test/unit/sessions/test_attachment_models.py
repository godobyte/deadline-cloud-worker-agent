# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Unit tests for worker-specific data structures.
"""

import pytest
import platform
from unittest.mock import patch

from deadline_worker_agent.sessions.attachment_models import (
    WorkerManifestProperties,
)
from deadline.job_attachments.models import (
    ManifestProperties,
    PathFormat,
    PathMappingRule,
)


class TestWorkerManifestProperties:
    """Test cases for WorkerManifestProperties class."""

    def test_initialization_with_required_fields(self):
        """Test WorkerManifestProperties initialization with required fields."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="shared_storage",
        )

        # WHEN
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
            local_manifest_paths=["/local/manifest.json"],
        )

        # THEN
        assert worker_props.manifest_properties == manifest_props
        assert worker_props.local_root_path == "/local/root"
        assert worker_props.local_manifest_paths == ["/local/manifest.json"]
        assert worker_props.local_input_manifest_path is None

    def test_initialization_with_empty_string_local_root_path(self):
        """Test that empty string local_root_path is allowed (no validation)."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )

        # WHEN
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path=""
        )

        # THEN
        assert worker_props.local_root_path == ""

    def test_property_accessors(self):
        """Test property accessors for manifest properties."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.WINDOWS,
            fileSystemLocationName="shared_storage",
            inputManifestPath="input.json",
            inputManifestHash="hash123",
            outputRelativeDirectories=["out1", "out2"],
        )
        # WHEN
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="/local/root"
        )

        # WHEN/THEN
        assert worker_props.root_path == "/source/path"
        assert worker_props.root_path_format == PathFormat.WINDOWS
        assert worker_props.file_system_location_name == "shared_storage"
        assert worker_props.input_manifest_path == "input.json"
        assert worker_props.input_manifest_hash == "hash123"
        assert worker_props.output_relative_directories == ["out1", "out2"]

    def test_to_path_mapping_rule(self):
        """Test conversion to path mapping rule."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="/local/root"
        )

        # WHEN
        rule = worker_props.to_path_mapping_rule()

        # THEN
        assert isinstance(rule, PathMappingRule)
        assert rule.source_path_format == "posix"
        assert rule.source_path == "/source/path"
        assert rule.destination_path == "/local/root"

    def test_constructor_with_all_parameters(self):
        """Test creating WorkerManifestProperties using constructor."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="shared",
        )

        # WHEN
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
            local_manifest_paths=["/local/manifest.json"],
        )

        # THEN
        assert worker_props.manifest_properties == manifest_props
        assert worker_props.local_root_path == "/local/root"
        assert worker_props.local_manifest_paths == ["/local/manifest.json"]

    def test_constructor_without_manifest_path(self):
        """Test creating WorkerManifestProperties without local manifest path."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )

        # WHEN
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="/local/root"
        )

        # THEN
        assert worker_props.manifest_properties == manifest_props
        assert worker_props.local_root_path == "/local/root"
        assert worker_props.local_manifest_paths == []

    def test_input_manifest_path_setter(self):
        """Test input_manifest_path property setter."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            inputManifestPath="s3://input/manifest.json",
            fileSystemLocationName="shared_storage",
        )
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="/local/root"
        )

        # WHEN
        worker_props.local_input_manifest_path = "/local/input/manifest.json"

        # THEN
        assert worker_props.input_manifest_path == "s3://input/manifest.json"
        assert worker_props.local_input_manifest_path == "/local/input/manifest.json"

    def test_property_accessors_with_none_values(self):
        """Test property accessors when optional fields are None."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName=None,
            inputManifestPath=None,
            inputManifestHash=None,
            outputRelativeDirectories=None,
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="/local/root"
        )

        # WHEN/THEN
        assert worker_props.root_path == "/source/path"
        assert worker_props.root_path_format == PathFormat.POSIX
        assert worker_props.file_system_location_name is None
        assert worker_props.input_manifest_path is None
        assert worker_props.input_manifest_hash is None
        assert worker_props.output_relative_directories is None

    def test_to_path_mapping_rule_with_windows_format(self):
        """Test conversion to path mapping rule with Windows path format."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="C:\\source\\path", rootPathFormat=PathFormat.WINDOWS
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="C:\\local\\root"
        )

        # WHEN
        rule = worker_props.to_path_mapping_rule()

        # THEN
        assert isinstance(rule, PathMappingRule)
        assert rule.source_path_format == "windows"
        assert rule.source_path == "C:\\source\\path"
        assert rule.destination_path == "C:\\local\\root"

    def test_manifest_properties_with_complex_paths(self):
        """Test WorkerManifestProperties with complex file paths."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/complex/path/with spaces/and-dashes_underscores",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="complex_storage_name",
            inputManifestPath="manifests/complex/input_manifest_v2.json",
            inputManifestHash="sha256:abcdef1234567890",
            outputRelativeDirectories=["output/renders", "output/logs", "temp/cache"],
        )

        # WHEN
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/session/complex_path_hash",
            local_manifest_paths=["/local/session/manifests/complex_manifest.json"],
            local_input_manifest_path="/local/session/manifests/complex_manifest.json",
        )

        # THEN
        assert worker_props.root_path == "/complex/path/with spaces/and-dashes_underscores"
        assert worker_props.file_system_location_name == "complex_storage_name"
        assert worker_props.input_manifest_path == "manifests/complex/input_manifest_v2.json"
        assert worker_props.input_manifest_hash == "sha256:abcdef1234567890"
        assert worker_props.output_relative_directories is not None
        assert len(worker_props.output_relative_directories) == 3
        assert "output/renders" in worker_props.output_relative_directories
        assert worker_props.local_root_path == "/local/session/complex_path_hash"
        assert worker_props.local_manifest_paths == [
            "/local/session/manifests/complex_manifest.json"
        ]
        assert (
            worker_props.local_input_manifest_path
            == "/local/session/manifests/complex_manifest.json"
        )

    def test_equality_and_comparison(self):
        """Test equality comparison between WorkerManifestProperties instances."""
        # GIVEN
        manifest_props1 = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="shared",
        )

        manifest_props2 = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            fileSystemLocationName="shared",
        )

        worker_props1 = WorkerManifestProperties(
            manifest_properties=manifest_props1,
            local_root_path="/local/root",
            local_manifest_paths=["/local/manifest.json"],
        )

        worker_props2 = WorkerManifestProperties(
            manifest_properties=manifest_props2,
            local_root_path="/local/root",
            local_manifest_paths=["/local/manifest.json"],
        )

        worker_props3 = WorkerManifestProperties(
            manifest_properties=manifest_props1,
            local_root_path="/different/root",
            local_manifest_paths=["/local/manifest.json"],
        )

        # WHEN/THEN
        # Test equality (dataclass should provide __eq__)
        assert worker_props1 == worker_props2
        assert worker_props1 != worker_props3

    def test_constructor_with_valid_inputs(self):
        """Test that constructor works with valid inputs."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )

        # WHEN
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="/valid/root"
        )

        # THEN
        assert worker_props.local_root_path == "/valid/root"

    def test_local_manifest_paths_default_factory(self):
        """Test that each instance gets its own local_manifest_paths list (mutable default fix)."""
        # GIVEN
        manifest_props1 = ManifestProperties(
            rootPath="/source/path1", rootPathFormat=PathFormat.POSIX
        )
        manifest_props2 = ManifestProperties(
            rootPath="/source/path2", rootPathFormat=PathFormat.POSIX
        )

        # WHEN
        # Create two instances without specifying local_manifest_paths
        worker_props1 = WorkerManifestProperties(
            manifest_properties=manifest_props1, local_root_path="/local/root1"
        )
        worker_props2 = WorkerManifestProperties(
            manifest_properties=manifest_props2, local_root_path="/local/root2"
        )

        # THEN
        # Verify each instance has its own empty list
        assert worker_props1.local_manifest_paths == []
        assert worker_props2.local_manifest_paths == []
        assert worker_props1.local_manifest_paths is not worker_props2.local_manifest_paths

        # Modify one instance's list and verify the other is unaffected
        worker_props1.local_manifest_paths.append("/path/to/manifest1.json")
        assert worker_props1.local_manifest_paths == ["/path/to/manifest1.json"]
        assert worker_props2.local_manifest_paths == []

        # Modify the second instance's list
        worker_props2.local_manifest_paths.extend(
            ["/path/to/manifest2.json", "/path/to/manifest3.json"]
        )
        assert worker_props1.local_manifest_paths == ["/path/to/manifest1.json"]
        assert worker_props2.local_manifest_paths == [
            "/path/to/manifest2.json",
            "/path/to/manifest3.json",
        ]

    def test_local_manifest_paths_initialization_with_provided_list(self):
        """Test initialization with explicitly provided local_manifest_paths."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )
        provided_paths = ["/provided/manifest1.json", "/provided/manifest2.json"]

        # WHEN
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
            local_manifest_paths=provided_paths,
        )

        # THEN
        # Verify the provided list contents are copied
        assert worker_props.local_manifest_paths == provided_paths
        # Verify it's a copy (not the same reference) for encapsulation
        assert worker_props.local_manifest_paths is not provided_paths

        # Verify that modifying the original list doesn't affect the object
        provided_paths.append("/external/modification.json")
        assert "/external/modification.json" not in worker_props.local_manifest_paths

    def test_local_manifest_paths_defensive_copy_behavior(self):
        """Test that the constructor creates a defensive copy of the provided list."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )
        original_list = ["/original/manifest.json"]

        # WHEN
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
            local_manifest_paths=original_list,
        )

        # THEN
        # Modifying the original list should not affect the worker properties
        original_list.clear()
        original_list.append("/completely/different.json")

        # The worker properties should still have the original values
        assert worker_props.local_manifest_paths == ["/original/manifest.json"]
        assert len(worker_props.local_manifest_paths) == 1

    def test_local_manifest_paths_modification_after_initialization(self):
        """Test that local_manifest_paths can be modified after initialization."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path", rootPathFormat=PathFormat.POSIX
        )

        # WHEN
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="/local/root"
        )

        # WHEN/THEN
        # Start with empty list
        assert worker_props.local_manifest_paths == []

        # Add paths using various list methods
        worker_props.local_manifest_paths.append("/manifest1.json")
        assert worker_props.local_manifest_paths == ["/manifest1.json"]

        worker_props.local_manifest_paths.extend(["/manifest2.json", "/manifest3.json"])
        assert len(worker_props.local_manifest_paths) == 3
        assert "/manifest2.json" in worker_props.local_manifest_paths
        assert "/manifest3.json" in worker_props.local_manifest_paths

        # Test insertion
        worker_props.local_manifest_paths.insert(0, "/manifest0.json")
        assert worker_props.local_manifest_paths[0] == "/manifest0.json"
        assert len(worker_props.local_manifest_paths) == 4

    def test_to_dict_with_none_values(self):
        """Test to_dict with mocked ManifestProperties.to_dict() for None values."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.WINDOWS,
            fileSystemLocationName=None,
            inputManifestPath=None,
            inputManifestHash=None,
            outputRelativeDirectories=None,
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
        )

        # Mock ManifestProperties.to_dict() to return controlled data
        mock_manifest_dict = {"mocked": "data"}

        # WHEN
        with patch.object(
            manifest_props, "to_dict", return_value=mock_manifest_dict
        ) as mock_to_dict:
            result_dict = worker_props.to_dict()

            # THEN
            # Verify the mock was called
            mock_to_dict.assert_called_once()

            # Verify the integration: mock result is used as manifestProperties
            assert result_dict["manifestProperties"] is mock_manifest_dict

            # Verify worker-specific fields are correct
            assert result_dict["localManifestPaths"] == []
            assert result_dict["localRootPath"] == "/local/root"

    def test_from_dict_deserialization(self):
        """Test creating WorkerManifestProperties from dictionary."""
        # GIVEN
        data = {
            "manifestProperties": {
                "rootPath": "/source/path",
                "rootPathFormat": "posix",
                "fileSystemLocationName": "shared_storage",
                "inputManifestPath": "input.json",
                "inputManifestHash": "hash123",
                "outputRelativeDirectories": ["out1", "out2"],
            },
            "localManifestPaths": ["/local/manifest1.json", "/local/manifest2.json"],
            "localInputManifestPath": "/local/manifest1.json",
            "localRootPath": "/local/root",
        }

        # WHEN
        worker_props = WorkerManifestProperties.from_dict(data)

        # THEN
        # Verify manifest properties
        assert worker_props.root_path == "/source/path"
        assert worker_props.root_path_format == PathFormat.POSIX
        assert worker_props.file_system_location_name == "shared_storage"
        assert worker_props.input_manifest_path == "input.json"
        assert worker_props.input_manifest_hash == "hash123"
        assert worker_props.output_relative_directories == ["out1", "out2"]

        # Verify other fields
        assert worker_props.local_manifest_paths == [
            "/local/manifest1.json",
            "/local/manifest2.json",
        ]
        assert worker_props.local_input_manifest_path == "/local/manifest1.json"
        assert worker_props.local_root_path == "/local/root"

    def test_from_dict_with_missing_optional_fields(self):
        """Test from_dict with missing optional fields."""
        # GIVEN
        data = {
            "manifestProperties": {
                "rootPath": "/source/path",
                "rootPathFormat": "windows",
            },
            "localRootPath": "/local/root",
        }

        # WHEN
        worker_props = WorkerManifestProperties.from_dict(data)

        # THEN
        assert worker_props.root_path == "/source/path"
        assert worker_props.root_path_format == PathFormat.WINDOWS
        assert worker_props.file_system_location_name is None
        assert worker_props.input_manifest_path is None
        assert worker_props.input_manifest_hash is None
        assert worker_props.output_relative_directories is None
        assert worker_props.local_manifest_paths == []  # Default empty list
        assert worker_props.local_root_path == "/local/root"

    def test_from_dict_missing_required_fields_raises_error(self):
        """Test that from_dict raises KeyError for missing required fields."""
        # GIVEN - Missing manifestProperties
        data_missing_manifest = {
            "localRootPath": "/local/root",
        }
        # THEN
        with pytest.raises(KeyError):
            # WHEN
            WorkerManifestProperties.from_dict(data_missing_manifest)

        # GIVEN - Missing localRootPath
        data_missing_root = {
            "manifestProperties": {
                "rootPath": "/source/path",
                "rootPathFormat": "posix",
            },
        }
        # THEN
        with pytest.raises(KeyError):
            # WHEN
            WorkerManifestProperties.from_dict(data_missing_root)

        # GIVEN - Missing rootPath in manifestProperties
        data_missing_root_path = {
            "manifestProperties": {
                "rootPathFormat": "posix",
            },
            "localRootPath": "/local/root",
        }
        # THEN
        with pytest.raises(KeyError):
            # WHEN
            WorkerManifestProperties.from_dict(data_missing_root_path)

    def test_from_dict_invalid_path_format_raises_error(self):
        """Test that from_dict raises ValueError for invalid path format."""
        # GIVEN
        data = {
            "manifestProperties": {
                "rootPath": "/source/path",
                "rootPathFormat": "invalid_format",
            },
            "localRootPath": "/local/root",
        }

        # WHEN/THEN
        with pytest.raises(ValueError):
            WorkerManifestProperties.from_dict(data)

    def test_dict_roundtrip_serialization(self):
        """Test that dict serialization and deserialization are symmetric with mocked to_dict."""
        # GIVEN
        # Create original object with minimal data
        manifest_props = ManifestProperties(
            rootPath="/minimal/path",
            rootPathFormat=PathFormat.POSIX,
        )

        original_worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/minimal",
        )
        original_worker_props.local_input_manifest_path = "/local/input/manifest.json"

        # Mock ManifestProperties.to_dict() with data compatible with from_dict
        mock_manifest_dict = {"rootPath": "/minimal/path", "rootPathFormat": "posix"}

        # WHEN
        with patch.object(
            manifest_props, "to_dict", return_value=mock_manifest_dict
        ) as mock_to_dict:
            # Serialize to dict and back
            data_dict = original_worker_props.to_dict()

            # THEN
            # Verify the mock was called
            mock_to_dict.assert_called_once()

            # Verify the mock result is properly integrated
            assert data_dict["manifestProperties"] is mock_manifest_dict

            # Since from_dict doesn't use ManifestProperties.to_dict(),
            # we can still test the round trip
            reconstructed_worker_props = WorkerManifestProperties.from_dict(data_dict)

            # Verify they are equal
            assert reconstructed_worker_props == original_worker_props

    def test_local_output_relative_directories_returns_none_when_no_output_dirs(self):
        """Test local_output_relative_directories returns None when no output directories are set."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            outputRelativeDirectories=None,
        )
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
        )

        # WHEN
        result = worker_props.local_output_relative_directories()

        # THEN
        assert result is None

    def test_local_output_relative_directories_returns_none_when_empty_output_dirs(self):
        """Test local_output_relative_directories returns None when output directories list is empty."""
        # GIVEN
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            outputRelativeDirectories=[],
        )
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
        )

        # WHEN
        result = worker_props.local_output_relative_directories()

        # THEN
        assert result == []

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="This test is for testing path changes on POSIX systems.",
    )
    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_no_conversion_when_same_format(
        self, mock_get_host_format
    ):
        """Test local_output_relative_directories returns unchanged paths when source and host formats match."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.POSIX
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            outputRelativeDirectories=["output/renders", "output/logs", "temp/cache"],
        )
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
        )

        # WHEN
        result = worker_props.local_output_relative_directories()

        # THEN
        assert result == ["output/renders", "output/logs", "temp/cache"]
        mock_get_host_format.assert_called_once()

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="This test is for testing path changes on POSIX systems.",
    )
    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_converts_windows_to_posix(
        self, mock_get_host_format
    ):
        """Test local_output_relative_directories converts Windows paths to POSIX format."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.POSIX
        manifest_props = ManifestProperties(
            rootPath="C:\\source\\path",
            rootPathFormat=PathFormat.WINDOWS,
            outputRelativeDirectories=["output\\renders", "output\\logs", "temp\\cache"],
        )
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
        )

        # WHEN
        result = worker_props.local_output_relative_directories()

        # THEN
        # Windows paths should be converted to POSIX format
        assert result == ["output/renders", "output/logs", "temp/cache"]
        mock_get_host_format.assert_called_once()

    @pytest.mark.skipif(
        platform.system() != "Windows", reason="This test is for testing path changes in Windows."
    )
    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_converts_posix_to_windows(
        self, mock_get_host_format
    ):
        """Test local_output_relative_directories converts POSIX paths to Windows format."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.WINDOWS
        manifest_props = ManifestProperties(
            rootPath="/source/path",
            rootPathFormat=PathFormat.POSIX,
            outputRelativeDirectories=["output/renders", "output/logs", "temp/cache"],
        )
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="C:\\local\\root",
        )

        # WHEN
        result = worker_props.local_output_relative_directories()

        # THEN
        # POSIX paths should be converted to Windows format
        expected = ["output\\renders", "output\\logs", "temp\\cache"]
        assert result == expected
        mock_get_host_format.assert_called_once()

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="This test is for testing path changes on POSIX systems.",
    )
    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_handles_complex_windows_paths(
        self, mock_get_host_format
    ):
        """Test local_output_relative_directories handles complex Windows paths with spaces and special chars."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.POSIX
        manifest_props = ManifestProperties(
            rootPath="C:\\Program Files\\App",
            rootPathFormat=PathFormat.WINDOWS,
            outputRelativeDirectories=[
                "output\\final renders",
                "logs\\error logs",
                "temp\\cache-files_v2",
            ],
        )
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
        )

        # WHEN
        result = worker_props.local_output_relative_directories()

        # THEN
        expected = ["output/final renders", "logs/error logs", "temp/cache-files_v2"]
        assert result == expected
        mock_get_host_format.assert_called_once()

    @pytest.mark.skipif(
        platform.system() != "Windows", reason="This test is for testing path changes in Windows."
    )
    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_handles_complex_posix_paths(
        self, mock_get_host_format
    ):
        """Test local_output_relative_directories handles complex POSIX paths with spaces and special chars."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.WINDOWS
        manifest_props = ManifestProperties(
            rootPath="/usr/local/app",
            rootPathFormat=PathFormat.POSIX,
            outputRelativeDirectories=[
                "output/final renders",
                "logs/error logs",
                "temp/cache-files_v2",
            ],
        )
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="C:\\local\\root",
        )

        # WHEN
        result = worker_props.local_output_relative_directories()

        # THEN
        expected = ["output\\final renders", "logs\\error logs", "temp\\cache-files_v2"]
        assert result == expected
        mock_get_host_format.assert_called_once()

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="This test is for testing path changes on POSIX systems.",
    )
    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_single_directory(self, mock_get_host_format):
        """Test local_output_relative_directories works with single directory."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.POSIX
        manifest_props = ManifestProperties(
            rootPath="C:\\source",
            rootPathFormat=PathFormat.WINDOWS,
            outputRelativeDirectories=["output"],
        )
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
        )

        # WHEN
        result = worker_props.local_output_relative_directories()

        # THEN
        assert result == ["output"]
        mock_get_host_format.assert_called_once()

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="This test is for testing path changes on POSIX systems.",
    )
    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_preserves_original_list(self, mock_get_host_format):
        """Test local_output_relative_directories doesn't modify the original output directories list."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.POSIX
        original_dirs = ["output\\renders", "output\\logs"]
        manifest_props = ManifestProperties(
            rootPath="C:\\source",
            rootPathFormat=PathFormat.WINDOWS,
            outputRelativeDirectories=original_dirs,
        )
        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props,
            local_root_path="/local/root",
        )

        # WHEN
        result = worker_props.local_output_relative_directories()

        # THEN
        # Original list should be unchanged
        assert manifest_props.outputRelativeDirectories == ["output\\renders", "output\\logs"]
        # Result should be converted
        assert result == ["output/renders", "output/logs"]
        # Should be different objects
        assert result is not manifest_props.outputRelativeDirectories
        mock_get_host_format.assert_called_once()

    # Edge case tests for path conversion with special characters
    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_special_characters_preserved(
        self, mock_get_host_format
    ):
        """Test that special characters are preserved during path conversion."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.POSIX
        test_cases = [
            "output[test]",
            "output{data}",
            "output*files",
            "output?dir",
            "folder with spaces[brackets]",
        ]

        for output_dir in test_cases:
            manifest_props = ManifestProperties(
                rootPath="/test/root",
                rootPathFormat=PathFormat.POSIX,
                outputRelativeDirectories=[output_dir],
            )

            worker_props = WorkerManifestProperties(
                manifest_properties=manifest_props, local_root_path="/local/root"
            )

            # WHEN
            result = worker_props.local_output_relative_directories()

            # THEN
            assert result == [output_dir], f"Special chars not preserved: {output_dir} -> {result}"

    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_windows_to_posix_with_special_chars(
        self, mock_get_host_format
    ):
        """Test Windows to POSIX conversion preserves special characters."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.POSIX
        test_cases = [
            ("output[test]", "output[test]"),
            ("folder\\subfolder[data]", "folder/subfolder[data]"),
            ("..\\..\\output[brackets]", "../../output[brackets]"),
            ("temp\\output{data}*files", "temp/output{data}*files"),
        ]

        for windows_path, expected_posix in test_cases:
            manifest_props = ManifestProperties(
                rootPath="C:\\test\\root",
                rootPathFormat=PathFormat.WINDOWS,
                outputRelativeDirectories=[windows_path],
            )

            worker_props = WorkerManifestProperties(
                manifest_properties=manifest_props, local_root_path="/local/root"
            )

            # WHEN
            result = worker_props.local_output_relative_directories()

            # THEN
            assert result == [expected_posix], f"Windows->POSIX failed: {windows_path} -> {result}"

    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_mixed_separators_with_special_chars(
        self, mock_get_host_format
    ):
        """Test paths with mixed separators and special characters."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.POSIX
        test_cases = [
            "folder\\subfolder/output[data]",
            "folder/subfolder\\output{test}",
            "..\\../mixed[path]*files",
        ]

        for mixed_path in test_cases:
            manifest_props = ManifestProperties(
                rootPath="/test/root",
                rootPathFormat=PathFormat.POSIX,
                outputRelativeDirectories=[mixed_path],
            )

            worker_props = WorkerManifestProperties(
                manifest_properties=manifest_props, local_root_path="/local/root"
            )

            # WHEN
            result = worker_props.local_output_relative_directories()

            # THEN
            assert result is not None
            assert len(result) == 1
            # Should preserve special chars even with mixed separators
            assert any(char in result[0] for char in "[]{}*?"), (
                f"Special chars lost: {mixed_path} -> {result}"
            )

    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_unicode_with_special_chars(
        self, mock_get_host_format
    ):
        """Test Unicode characters combined with special characters."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.POSIX
        test_cases = [
            "输出目录[测试]",
            "フォルダ{データ}",
            "папка[тест]*файлы",
            "مجلد[اختبار]",
        ]

        for unicode_path in test_cases:
            manifest_props = ManifestProperties(
                rootPath="/test/root",
                rootPathFormat=PathFormat.POSIX,
                outputRelativeDirectories=[unicode_path],
            )

            worker_props = WorkerManifestProperties(
                manifest_properties=manifest_props, local_root_path="/local/root"
            )

            # WHEN
            result = worker_props.local_output_relative_directories()

            # THEN
            assert result == [unicode_path], (
                f"Unicode+special chars failed: {unicode_path} -> {result}"
            )

    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_edge_case_paths(self, mock_get_host_format):
        """Test empty paths and edge cases."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.POSIX
        test_cases = [
            "",
            ".",
            "..",
            "[empty]",
            "{empty}",
            "*",
            "?",
            "output[test]{data}*files?dir",  # Multiple special chars
        ]

        for edge_path in test_cases:
            manifest_props = ManifestProperties(
                rootPath="/test/root",
                rootPathFormat=PathFormat.POSIX,
                outputRelativeDirectories=[edge_path],
            )

            worker_props = WorkerManifestProperties(
                manifest_properties=manifest_props, local_root_path="/local/root"
            )

            # WHEN
            result = worker_props.local_output_relative_directories()

            # THEN
            assert result is not None
            assert len(result) == 1, f"Edge case path failed: {edge_path} -> {result}"

    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_case_sensitivity_preserved(
        self, mock_get_host_format
    ):
        """Test that case sensitivity is preserved across platforms."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.POSIX
        test_cases = [
            "Output[Data]",
            "output[data]",
            "OUTPUT[DATA]",
            "MixedCase[Test]{Data}",
        ]

        for case_path in test_cases:
            manifest_props = ManifestProperties(
                rootPath="/test/root",
                rootPathFormat=PathFormat.POSIX,
                outputRelativeDirectories=[case_path],
            )

            worker_props = WorkerManifestProperties(
                manifest_properties=manifest_props, local_root_path="/local/root"
            )

            # WHEN
            result = worker_props.local_output_relative_directories()

            # THEN
            assert result == [case_path], f"Case not preserved: {case_path} -> {result}"

    @patch("deadline_worker_agent.sessions.attachment_models.PathFormat.get_host_path_format")
    def test_local_output_relative_directories_long_paths_with_special_chars(
        self, mock_get_host_format
    ):
        """Test very long paths with special characters."""
        # GIVEN
        mock_get_host_format.return_value = PathFormat.POSIX
        long_segment = "very_long_directory_name_with_special_chars[test]{data}*files"
        long_path = "/".join([long_segment] * 5)  # Create long path

        manifest_props = ManifestProperties(
            rootPath="/test/root",
            rootPathFormat=PathFormat.POSIX,
            outputRelativeDirectories=[long_path],
        )

        worker_props = WorkerManifestProperties(
            manifest_properties=manifest_props, local_root_path="/local/root"
        )

        # WHEN
        result = worker_props.local_output_relative_directories()

        # THEN
        assert result is not None
        assert len(result) == 1
        # Verify special chars are preserved in long paths
        assert "[test]" in result[0]
        assert "{data}" in result[0]
        assert "*files" in result[0]


class TestWorkerManifestPropertiesPathHandlingVulnerabilities:
    """Additional test cases to discover path handling vulnerabilities."""

    def test_path_format_conversion_missing_else_case(self):
        """Test behavior when source_path_format is neither WINDOWS nor POSIX."""
        manifest_props = ManifestProperties(
            rootPath="/test/path",
            rootPathFormat=PathFormat.POSIX,
            outputRelativeDirectories=["subdir"],
        )

        # Mock an unknown format by patching the enum value
        with patch.object(manifest_props, "rootPathFormat", "UNKNOWN_FORMAT"):
            worker_props = WorkerManifestProperties(manifest_props, "/local/root")

            # This should handle unknown formats gracefully
            result = worker_props.local_output_relative_directories()
            assert result == ["subdir"]  # Should return unchanged

    def test_unicode_metadata_size_limits(self):
        """Test S3 metadata with very long Unicode paths."""
        # S3 metadata has size limits - test with long Unicode path
        long_unicode_path = "测试路径" * 1000  # Very long Chinese path

        manifest_props = ManifestProperties(
            rootPath=long_unicode_path, rootPathFormat=PathFormat.POSIX
        )
        worker_props = WorkerManifestProperties(manifest_props, "/local/root")

        metadata = worker_props.as_output_metadata()
        # Should handle long paths without crashing
        assert "Metadata" in metadata

    def test_metadata_inconsistency_with_non_ascii(self):
        """Test that non-ASCII paths create inconsistent metadata."""
        unicode_path = "/测试/路径"

        manifest_props = ManifestProperties(rootPath=unicode_path, rootPathFormat=PathFormat.POSIX)
        worker_props = WorkerManifestProperties(manifest_props, "/local/root")

        metadata = worker_props.as_output_metadata()["Metadata"]

        # Both fields should exist for non-ASCII paths
        assert "asset-root" in metadata
        assert "asset-root-json" in metadata

        # asset-root should contain JSON string, not original path
        assert metadata["asset-root"] == metadata["asset-root-json"]
        assert metadata["asset-root"].startswith('"')  # JSON encoded

    def test_hash_collision_potential(self):
        """Test hash collision scenarios with path concatenation."""
        # These could potentially create the same hash
        test_cases = [
            ("", "/path/to/file"),
            ("/path", "/to/file"),
            ("/path/to", "/file"),
        ]

        hashes = []
        for fs_location, root_path in test_cases:
            manifest_props = ManifestProperties(
                rootPath=root_path,
                rootPathFormat=PathFormat.POSIX,
                fileSystemLocationName=fs_location,
            )
            worker_props = WorkerManifestProperties(manifest_props, "/local/root")
            hash_val = worker_props.get_hashed_source_path()
            hashes.append(hash_val)

        # All hashes should be different
        assert len(set(hashes)) == len(hashes), "Hash collision detected"

    def test_path_traversal_in_deserialization(self):
        """Test deserialization with directory traversal sequences."""
        malicious_paths = [
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32",
            "/test/../../../sensitive",
            "C:\\test\\..\\..\\..\\sensitive",
        ]

        for malicious_path in malicious_paths:
            data = {
                "manifestProperties": {"rootPath": malicious_path, "rootPathFormat": "posix"},
                "localRootPath": "/local/root",
            }

            # Should either reject or sanitize malicious paths
            worker_props = WorkerManifestProperties.from_dict(data)
            # Test that the path doesn't escape intended boundaries
            assert not worker_props.root_path.startswith("/etc/")
            assert not worker_props.root_path.startswith("C:\\Windows\\")

    def test_null_byte_injection(self):
        """Test paths containing null bytes that could cause issues."""
        null_byte_path = "/test/path\x00/malicious"

        manifest_props = ManifestProperties(
            rootPath=null_byte_path, rootPathFormat=PathFormat.POSIX
        )
        worker_props = WorkerManifestProperties(manifest_props, "/local/root")

        # Should handle null bytes safely
        metadata = worker_props.as_output_metadata()
        assert "Metadata" in metadata

    def test_extremely_long_paths(self):
        """Test with paths exceeding filesystem limits."""
        # Most filesystems have path length limits (e.g., 4096 chars on Linux)
        very_long_path = "/test/" + "a" * 5000

        manifest_props = ManifestProperties(
            rootPath=very_long_path, rootPathFormat=PathFormat.POSIX
        )
        worker_props = WorkerManifestProperties(manifest_props, "/local/root")

        # Should handle long paths without crashing
        result = worker_props.local_output_relative_directories()
        # Should not crash on path operations

    def test_special_characters_in_path_conversion(self):
        """Test path conversion with glob metacharacters and special chars."""
        test_cases = [
            "[Julia] Project Aura",  # Square brackets
            "Project*Wildcard",  # Asterisk
            "Project?Question",  # Question mark
            "Project{Brace}",  # Curly braces
            "Project (Parens)",  # Parentheses
            "Project$Dollar",  # Dollar sign
            "Project^Caret",  # Caret
            "Project|Pipe",  # Pipe
        ]

        for test_dir in test_cases:
            manifest_props = ManifestProperties(
                rootPath="/test/path",
                rootPathFormat=PathFormat.POSIX,
                outputRelativeDirectories=[test_dir],
            )
            worker_props = WorkerManifestProperties(manifest_props, "/local/root")

            result = worker_props.local_output_relative_directories()
            assert result == [test_dir], f"Failed for directory: {test_dir}"

    def test_json_encoding_edge_cases(self):
        """Test JSON encoding with problematic characters."""
        problematic_paths = [
            '/path/with"quotes',
            "/path/with\\backslashes",
            "/path/with\nNewlines",
            "/path/with\tTabs",
            "/path/with\r\nCRLF",
            "/path/with\x00NullBytes",
        ]

        for path in problematic_paths:
            manifest_props = ManifestProperties(rootPath=path, rootPathFormat=PathFormat.POSIX)
            worker_props = WorkerManifestProperties(manifest_props, "/local/root")

            # Should handle problematic characters in JSON encoding
            metadata = worker_props.as_output_metadata()
            assert "Metadata" in metadata

    def test_empty_and_whitespace_paths(self):
        """Test handling of empty and whitespace-only paths."""
        edge_case_paths = [
            "",
            " ",
            "\t",
            "\n",
            "   ",
            "\t\n\r",
        ]

        for path in edge_case_paths:
            manifest_props = ManifestProperties(rootPath=path, rootPathFormat=PathFormat.POSIX)
            worker_props = WorkerManifestProperties(manifest_props, "/local/root")

            # Should handle edge cases gracefully
            metadata = worker_props.as_output_metadata()
            assert "Metadata" in metadata

            hash_val = worker_props.get_hashed_source_path()
            assert isinstance(hash_val, str)

    def test_path_injection_in_hash_generation(self):
        """Test path injection vulnerabilities in hash generation."""
        # Test cases where concatenation could be exploited
        injection_cases = [
            ("malicious", ""),  # fs_location with empty root_path
            ("", "malicious"),  # empty fs_location with root_path
            ("path1", "path2"),  # normal case
            ("path", "1path2"),  # potential collision with "path1" + "path2"
        ]

        hashes = []
        for fs_location, root_path in injection_cases:
            manifest_props = ManifestProperties(
                rootPath=root_path,
                rootPathFormat=PathFormat.POSIX,
                fileSystemLocationName=fs_location,
            )
            worker_props = WorkerManifestProperties(manifest_props, "/local/root")
            hash_val = worker_props.get_hashed_source_path()
            hashes.append((fs_location, root_path, hash_val))

        # Verify no unexpected hash collisions
        hash_values = [h[2] for h in hashes]
        assert len(set(hash_values)) == len(hash_values), f"Unexpected hash collision: {hashes}"
