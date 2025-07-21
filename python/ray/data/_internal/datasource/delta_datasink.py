import json
import logging
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from deltalake import (
    CommitProperties,
    PostCommitHookProperties,
    WriterProperties,
)
from deltalake.transaction import AddAction as DeltaAddAction
from deltalake.schema import Schema as DeltaSchema
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Union,
)
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.fs as pa_fs
from packaging.version import parse as parse_version
from pyarrow.parquet import FileMetaData

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray._raylet import ObjectRef
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.file_datasink import _FileDatasink


logger = logging.getLogger(__name__)


class WriteMode(Enum):
    # Error if the table already exists
    ERROR = "error"
    # Append to the table if it exists
    APPEND = "append"
    # Overwrite the table if it exists
    OVERWRITE = "overwrite"
    # Ignore the write if the table already exists
    IGNORE = "ignore"
    # Merge the table if it exists
    MERGE = "merge"


class MergeMode(Enum):
    """Merge operation modes."""

    UPSERT = "upsert"  # Insert if not exists, update if exists
    SCD_TYPE_1 = "scd_type_1"  # Slowly Changing Dimensions Type 1 (overwrite)
    SCD_TYPE_2 = "scd_type_2"  # Slowly Changing Dimensions Type 2 (versioning)
    INSERT_ONLY = "insert_only"  # Insert only new records
    UPDATE_ONLY = "update_only"  # Update only existing records
    DELETE = "delete"  # Delete matching records
    CUSTOM = "custom"  # Custom merge logic with user-defined conditions


class OptimizationMode(Enum):
    """Table optimization modes."""

    COMPACT = "compact"  # File compaction
    Z_ORDER = "z_order"  # Z-order optimization
    VACUUM = "vacuum"  # Remove old files


@dataclass
class AddAction:
    path: str
    size: int
    partition_values: Mapping[str, Optional[str]]
    modification_time: int
    data_change: bool
    stats: str


@dataclass
class SCDConfig:
    """Configuration for Slowly Changing Dimensions operations."""

    scd_type: Optional[int] = None  # 1, 2, or 3
    key_columns: List[str] = None  # Business key columns for matching

    # SCD Type 1 settings
    overwrite_columns: Optional[List[str]] = None  # Columns to overwrite in Type 1

    # SCD Type 2 settings
    version_column: str = "version"  # Version tracking column
    current_flag_column: str = "is_current"  # Current record flag
    start_time_column: str = "start_time"  # Valid from timestamp
    end_time_column: str = "end_time"  # Valid to timestamp
    hash_columns: Optional[List[str]] = None  # Columns to hash for change detection

    # SCD Type 3 settings
    previous_value_suffix: str = "_previous"  # Suffix for previous value columns
    change_columns: Optional[List[str]] = None  # Columns to track changes for

    def __post_init__(self):
        """Validate SCD configuration."""
        if self.scd_type is not None and self.scd_type not in [1, 2, 3]:
            raise ValueError("scd_type must be 1, 2, or 3")

        if self.scd_type in [1, 2, 3] and not self.key_columns:
            raise ValueError(f"key_columns required for SCD Type {self.scd_type}")


@dataclass
class MergeConditions:
    """Configuration for merge operation conditions (Databricks-style)."""

    # Main merge predicate
    merge_predicate: str

    # When matched conditions
    when_matched_update: Optional[Dict[str, str]] = None  # column -> expression
    when_matched_update_condition: Optional[
        str
    ] = None  # Additional condition for update
    when_matched_delete: bool = False
    when_matched_delete_condition: Optional[str] = None  # Condition for delete

    # When not matched by target conditions
    when_not_matched_insert: Optional[Dict[str, str]] = None  # column -> expression
    when_not_matched_insert_condition: Optional[str] = None  # Condition for insert

    # When not matched by source conditions
    when_not_matched_by_source_update: Optional[Dict[str, str]] = None
    when_not_matched_by_source_update_condition: Optional[str] = None
    when_not_matched_by_source_delete: bool = False
    when_not_matched_by_source_delete_condition: Optional[str] = None


@dataclass
class MergeConfig:
    """Enhanced configuration for merge operations with Databricks-style functionality."""

    # Basic configuration
    source_alias: str = "source"
    target_alias: str = "target"
    error_on_type_mismatch: bool = True

    # Retry configuration for concurrent write conflicts
    max_retry_attempts: int = 3  # Maximum number of retry attempts for merge conflicts
    base_retry_delay: float = 0.1  # Base delay in seconds between retries (exponential backoff)
    max_retry_delay: float = 5.0  # Maximum delay in seconds between retries

    # SCD configuration (optional)
    scd_config: Optional[SCDConfig] = None

    # Databricks-style merge conditions (optional)
    merge_conditions: Optional[MergeConditions] = None

    # Simple upsert configuration (legacy compatibility)
    mode: MergeMode = MergeMode.UPSERT
    predicate: Optional[str] = None
    update_columns: Optional[Dict[str, str]] = None
    insert_columns: Optional[Dict[str, str]] = None
    delete_predicate: Optional[str] = None

    def __post_init__(self):
        """Validate merge configuration."""
        if self.scd_config and self.merge_conditions:
            raise ValueError("Cannot specify both scd_config and merge_conditions")
        
        # Validate retry parameters
        if self.max_retry_attempts < 0:
            raise ValueError(f"max_retry_attempts must be non-negative, got {self.max_retry_attempts}")
        if self.base_retry_delay < 0:
            raise ValueError(f"base_retry_delay must be non-negative, got {self.base_retry_delay}")
        if self.max_retry_delay < self.base_retry_delay:
            raise ValueError(f"max_retry_delay ({self.max_retry_delay}) must be >= base_retry_delay ({self.base_retry_delay})")
        if self.max_retry_attempts > 10:
            import warnings
            warnings.warn(
                f"max_retry_attempts={self.max_retry_attempts} is very high and may cause long delays. "
                "Consider using 3-5 retries for most use cases.",
                UserWarning
            )


@dataclass
class OptimizationConfig:
    """
    Configuration for table optimization operations.

    Note: For vacuum operations, files are always deleted (no dry-run mode).
    Use the standalone vacuum_delta_table function for preview/dry-run functionality.
    """

    mode: OptimizationMode = OptimizationMode.COMPACT
    target_size_bytes: Optional[int] = None  # Target file size for compaction
    z_order_columns: Optional[List[str]] = None  # Columns for Z-order optimization
    max_concurrent_tasks: Optional[int] = None  # Max parallel tasks
    min_commit_interval: Optional[Union[int, timedelta]] = None  # Commit interval
    partition_filters: Optional[List[Tuple[str, str, Any]]] = None  # Partition filters
    retention_hours: Optional[int] = None  # Vacuum retention period
    max_spill_size: int = 21474836480  # 20GB default for Z-order


@dataclass
class DeltaSinkWriteResult:
    actions: List[AddAction]
    schema: Optional[pa.Schema] = None
    merge_metrics: Optional[Dict[str, Any]] = None
    optimization_metrics: Optional[Dict[str, Any]] = None


class DeltaJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, bytes):
            return obj.decode("unicode_escape", "backslashreplace")
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return str(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class AWSUtilities:
    """Utility class for AWS credential management and configuration."""

    @staticmethod
    def _get_aws_credentials():
        # TODO: This is a temporary fix to get the region from the cluster resources,
        # TODO: This should be replaced with a proper implementation as part of a shared Anyscale utils library
        from boto3 import Session

        # Grab the credentials from the current session, needed to authenticate the Delta Lake library
        session = Session()
        credentials = session.get_credentials()
        return credentials

    @staticmethod
    def _get_aws_region():
        # TODO: This is a temporary fix to get the region from the cluster resources,
        # TODO: This should be replaced with a proper implementation as part of a shared Anyscale utils library
        # Run in the if block to avoid the reinit step and logging
        if not ray.is_initialized():
            ray.init()

        # The cleanest apparent way of grabbing the current region is through the naming convention
        # of the cluster resources. The region is stored in the key like "anyscale/region:us-west-2"

        # Somehow Boto3 doesn't have a clean way of grabbing the current region??
        cluster_resources = ray.cluster_resources()
        region = [
            k.split(":")[1]
            for k in cluster_resources.keys()
            if k.startswith("anyscale/region:")
        ]
        region = region[0] if len(region) > 0 else None
        return region


class GCPUtilities:
    """Utility class for Google Cloud Platform credential management and configuration."""

    @staticmethod
    def _get_gcp_credentials():
        """
        Get GCP credentials using google-auth library.

        Returns:
            Credentials object and project ID
        """
        try:
            from google.auth import default
            from google.auth.exceptions import DefaultCredentialsError

            try:
                credentials, project_id = default()
                return credentials, project_id
            except DefaultCredentialsError:
                logger.warning(
                    "No GCP credentials found - this is expected if accessing public data or using explicit storage_options. "
                    "For private GCP resources, ensure you have: 1) Set GOOGLE_APPLICATION_CREDENTIALS environment variable, "
                    "2) Run 'gcloud auth application-default login', or 3) Use a service account key file."
                )
                return None, None

        except ImportError:
            logger.warning(
                "GCP integration requires google-auth library. Install with: pip install google-auth. "
                "If you're not using Google Cloud Storage, this warning can be safely ignored."
            )
            return None, None

    @staticmethod
    def _get_gcp_project_id():
        """
        Get GCP project ID from various sources.

        Returns:
            Project ID string or None
        """
        import os

        # Try environment variable first
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
        if project_id:
            return project_id

        # Try to get from default credentials
        try:
            from google.auth import default
            _, project_id = default()
            return project_id
        except Exception:
            pass

        # Try metadata service if running on GCP
        try:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            session = requests.Session()
            retry_strategy = Retry(total=3, backoff_factor=0.1)
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)

            response = session.get(
                "http://metadata.google.internal/computeMetadata/v1/project/project-id",
                headers={"Metadata-Flavor": "Google"},
                timeout=1
            )
            if response.status_code == 200:
                return response.text
        except Exception:
            pass

        # Try Ray cluster resources for Anyscale
        if ray.is_initialized():
            cluster_resources = ray.cluster_resources()
            project_resources = [
                k.split(":")[1]
                for k in cluster_resources.keys()
                if k.startswith("anyscale/gcp-project:")
            ]
            if project_resources:
                return project_resources[0]

        return None


class AzureUtilities:
    """Utility class for Microsoft Azure credential management and configuration."""

    @staticmethod
    def _get_azure_credentials():
        """
        Get Azure credentials using azure-identity library.

        Returns:
            Credentials object
        """
        try:
            from azure.identity import DefaultAzureCredential
            from azure.core.exceptions import ClientAuthenticationError

            try:
                credential = DefaultAzureCredential()
                # Test the credential by attempting to get a token
                credential.get_token("https://storage.azure.com/.default")
                return credential
            except ClientAuthenticationError:
                logger.warning(
                    "No Azure credentials found - this is expected if accessing public data or using explicit storage_options. "
                    "For private Azure resources, ensure you have: 1) Set Azure environment variables (AZURE_CLIENT_ID, etc.), "
                    "2) Run 'az login' with Azure CLI, 3) Use managed identity on Azure VMs, or 4) Configure service principal credentials."
                )
                return None

        except ImportError:
            logger.warning(
                "Azure integration requires azure-identity library. Install with: pip install azure-identity. "
                "If you're not using Azure storage, this warning can be safely ignored."
            )
            return None

    @staticmethod
    def _get_azure_account_info():
        """
        Get Azure storage account information from environment or Ray cluster.

        Returns:
            Dictionary with account_name, account_key, and other relevant settings
        """
        import os

        account_info = {}

        # Try environment variables
        account_info["account_name"] = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        account_info["account_key"] = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        account_info["sas_token"] = os.getenv("AZURE_STORAGE_SAS_TOKEN")
        account_info["tenant_id"] = os.getenv("AZURE_TENANT_ID")
        account_info["client_id"] = os.getenv("AZURE_CLIENT_ID")
        account_info["client_secret"] = os.getenv("AZURE_CLIENT_SECRET")

        # Try Ray cluster resources for Anyscale
        if ray.is_initialized():
            cluster_resources = ray.cluster_resources()

            # Look for Azure-related resources
            azure_resources = {
                k.split(":", 2)[-1]: k.split(":", 1)[1]
                for k in cluster_resources.keys()
                if k.startswith("anyscale/azure:")
            }

            for key, value in azure_resources.items():
                if key == "storage-account":
                    account_info["account_name"] = value
                elif key == "tenant":
                    account_info["tenant_id"] = value
                elif key == "subscription":
                    account_info["subscription_id"] = value

        # Filter out None values
        return {k: v for k, v in account_info.items() if v is not None}

    @staticmethod
    def _get_azure_region():
        """
        Get Azure region from environment or Ray cluster.

        Returns:
            Region string or None
        """
        import os

        # Try environment variable
        region = os.getenv("AZURE_REGION") or os.getenv("AZURE_LOCATION")
        if region:
            return region

        # Try Ray cluster resources for Anyscale
        if ray.is_initialized():
            cluster_resources = ray.cluster_resources()
            region_resources = [
                k.split(":")[1]
                for k in cluster_resources.keys()
                if k.startswith("anyscale/azure-region:")
            ]
            if region_resources:
                return region_resources[0]

        return None


def try_get_deltatable(
    table_uri: Union[str, Path], storage_options: Optional[Dict[str, str]]
):
    """
    Attempts to get a DeltaTable from the given URI.
    """
    from deltalake.table import DeltaTable

    try:
        return DeltaTable(table_uri, storage_options=storage_options)
    except Exception as e:
        return None


@dataclass
class DeltaWriteConfig:
    """
    Configuration object for writing data to a DeltaTable.
    All parameters default to None unless specified.
    """

    # --- Required for file/block writing ---
    schema: Optional[pa.Schema] = None

    # --- Partitioning ---
    partition_cols: Optional[List[str]] = None
    partition_by: Optional[Union[List[str], str]] = None
    partition_filters: Optional[List[Tuple[str, str, Any]]] = None

    # --- File writing options ---
    file_options: Optional[Any] = None  # e.g. ParquetFileWriteOptions
    max_partitions: Optional[int] = None
    max_open_files: int = 1024
    max_rows_per_file: int = 10 * 1024 * 1024
    max_rows_per_group: int = 128 * 1024
    min_rows_per_group: int = 64 * 1024

    # --- Table (delta metadata) ---
    name: Optional[str] = None
    description: Optional[str] = None
    configuration: Optional[Mapping[str, Optional[str]]] = None

    # --- Schema/overwrite ---
    overwrite_schema: bool = (
        False  # When used, mapped to schema_mode='overwrite' in new API
    )

    # --- Storage/engine ---
    storage_options: Optional[Dict[str, str]] = None
    engine: Literal["pyarrow", "rust"] = "rust"
    large_dtypes: bool = False
    writer_properties: Optional[WriterProperties] = None  # Native Delta/WritersOptions
    predicate: Optional[str] = None  # Overwrite predicate
    target_file_size: Optional[int] = None

    # --- Commit & transaction ---
    commit_properties: Optional[CommitProperties] = None
    post_commithook_properties: Optional[PostCommitHookProperties] = None

    @classmethod
    def from_dict(cls, config_dict: dict):
        """
        Create a DeltaWriteConfig from a dictionary. Only sets attributes present in the class.
        """
        obj = cls()
        for key, val in config_dict.items():
            if hasattr(obj, key):
                setattr(obj, key, val)
            else:
                raise ValueError(f"Unknown config key: {key}")
        return obj


class DeltaUtilities:
    def __init__(
        self,
        delta_uri: str,
        schema: pa.schema,
        filesystem: pa_fs.FileSystem,
        partition_cols: Optional[List[str]] = None,
        max_partitions: Optional[int] = None,
        config: Optional[DeltaWriteConfig] = None,
    ) -> None:
        """
        Handles writing data batches to a Delta Table

        Examples:
            .. testcode::
                :skipif: True

                writer = DeltaDatasetWriter(
                    delta_uri="s3://bucket/path/to/delta-table",
                    schema=schema,
                    filesystem=fs,
                    partition_cols=["country", "date"]
                )
                add_actions = writer.write(batch_ref, file_options=file_opts)

        Args:
            delta_uri: The root URI of the Delta table.
            schema: The expected Arrow schema of the data.
            filesystem: The Arrow-compatible filesystem object.
            partition_cols: Columns to partition by; optional.
            max_partitions: Upper bound for file parallelism (default 128).
            config: Optional DeltaConfig object
        """
        self.delta_uri = delta_uri
        self.schema = schema
        self.filesystem = filesystem
        self.partition_cols = partition_cols or []
        self.max_partitions = max_partitions
        self._init_partitioning()
        self.write_config = config

    def _init_partitioning(self) -> None:
        """
        Initialize partitioning for PyArrow based on partition_cols.
        """
        if self.partition_cols:
            partition_schema = pa.schema(
                [
                    (name, typ)
                    for name, typ in zip(self.schema.names, self.schema.types)
                    if name in self.partition_cols
                ]
            )
            self.partitioning = pa_ds.partitioning(partition_schema, flavor="hive")
        else:
            self.partitioning = None

    def _get_add_action_for_written_file(
        self, written_file: Any, delta_uri: str
    ) -> Any:
        """
        Converts a written file record into an AddAction object.
        You may want to adapt this for your AddAction and file stats logic.

        Args:
            written_file: Object from PyArrow's file_visitor.
            delta_uri: Root URI of the Delta table.

        Returns:
            An AddAction object.
        """
        # Convert absolute file path to relative path from Delta table root
        written_path = written_file.path
        delta_root = urlparse(delta_uri).path

        # Make path relative to Delta table root
        if written_path.startswith(delta_root):
            relative_path = written_path[len(delta_root) :].lstrip("/")
        else:
            # Fallback: use the full path
            relative_path = written_path

        # Extract partition information from the relative path
        _, partition_values = self._get_partitions_from_path(relative_path)

        # For Delta Lake, store the relative path (no scheme needed for local files)
        if urlparse(delta_uri).scheme in ("", "file", None):
            path = relative_path  # Keep as relative path for local files
        else:
            # For cloud storage, add the appropriate scheme
            path = self._add_scheme_to_path(urlparse(delta_uri).scheme, relative_path)
        stats = self._get_file_stats_from_metadata(written_file.metadata)

        # Get file size with multiple approaches
        size = None

        # Primary: try written_file.size attribute (available in modern PyArrow)
        size = getattr(written_file, "size", None)

        # Fallback: try filesystem
        if size is None:
            try:
                file_info = self.filesystem.get_file_info([path])
                if file_info and len(file_info) > 0:
                    size = file_info[0].size
            except Exception:
                pass

        # Fallback: try metadata
        if size is None and written_file.metadata:
            try:
                # Calculate size from metadata if available
                total_compressed_size = 0
                for row_group_idx in range(written_file.metadata.num_row_groups):
                    rg = written_file.metadata.row_group(row_group_idx)
                    total_compressed_size += rg.total_compressed_size
                size = total_compressed_size
            except Exception:
                pass

        # Final fallback: use 0 (should not happen in production)
        if size is None:
            size = 0
        add_action = AddAction(
            path=path,
            size=size,
            partition_values=partition_values,
            modification_time=int(datetime.now().timestamp() * 1000),
            data_change=True,
            stats=json.dumps(stats, cls=DeltaJSONEncoder),
        )
        return add_action

    @staticmethod
    def _validate_schema(
        schema: pa.Schema,
        partition_cols: Optional[List[str]] = None,
    ) -> None:
        """
        Validates the schema against the partition columns.
        Args:
            schema: The schema to validate.
            partition_cols: The partition columns to validate against.
        Raises:
            ValueError: If the schema is invalid.
        """
        if partition_cols is not None:
            for col in partition_cols:
                if col not in schema.names:
                    raise ValueError(f"Partition column {col} not found in schema.")

    @staticmethod
    def _add_scheme_to_path(scheme: str, path: str) -> str:
        """Ensure the path has the correct URI scheme.

        Args:
            scheme: File format scheme
            path: The path to parse
        Returns:
            String of the path with appropriate scheme (or no scheme for local files)
        """
        parsed = urlparse(path)
        if parsed.scheme:
            return path

        # For local filesystem paths, don't add a scheme - Delta Lake prefers plain paths
        if scheme == "" or scheme is None or scheme == "file":
            # Ensure proper absolute path format
            if not path.startswith("/"):
                path = "/" + path
            return path

        # For other schemes (s3, gcs, etc.), add the scheme
        if not path.startswith("/"):
            path = "/" + path
        return f"{scheme}://{path}"

    @staticmethod
    def _get_partitions_from_path(path: str) -> Tuple[str, Dict[str, Optional[str]]]:
        """
        Parses the path to extract partition information.

        Args:
            path: The path to parse.

        Returns:
            A tuple containing the path and a dictionary of partition values.
        """
        if path[0] == "/":
            path = path[1:]
        parts = path.split("/")
        parts.pop()  # remove filename
        out: Dict[str, Optional[str]] = {}

        for part in parts:
            if part == "":
                continue
            if "=" in part:
                key, value = part.split("=", maxsplit=1)
                if value == "__HIVE_DEFAULT_PARTITION__":
                    out[key] = None
                else:
                    out[key] = value
        return path, out

    @staticmethod
    def _get_file_stats_from_metadata(
        metadata: FileMetaData,
    ) -> Dict[str, Union[int, Dict[str, Any]]]:
        """
        Extracts statistics from the metadata.

        Args:
            metadata: The metadata to extract statistics from.

        Returns:
            dictionary containing the statistics.
        """
        stats = {
            "numRecords": metadata.num_rows,
            "minValues": {},
            "maxValues": {},
            "nullCount": {},
        }

        def iter_groups(metadata) -> Iterator[Any]:
            for i in range(metadata.num_row_groups):
                yield metadata.row_group(i)

        for column_idx in range(metadata.num_columns):
            if metadata.num_row_groups > 0:
                name = metadata.row_group(0).column(column_idx).path_in_schema
                # If stats missing, then we can't know aggregate stats
                if all(
                    group.column(column_idx).is_stats_set
                    for group in iter_groups(metadata)
                ):
                    stats["nullCount"][name] = sum(
                        group.column(column_idx).statistics.null_count
                        for group in iter_groups(metadata)
                    )

                    # Min / max may not exist for some column types, or if all values are null
                    if any(
                        group.column(column_idx).statistics.has_min_max
                        for group in iter_groups(metadata)
                    ):
                        # Min and Max are recorded in physical type, not logical type
                        # https://stackoverflow.com/questions/66753485/decoding-parquet-min-max-statistics-for-decimal-type
                        # TODO: Add logic to decode physical type for DATE, DECIMAL
                        logical_type = (
                            metadata.row_group(0)
                            .column(column_idx)
                            .statistics.logical_type.type
                        )
                        if get_pyarrow_version() < parse_version(
                            "8.0.0"
                        ) and logical_type not in [
                            "STRING",
                            "INT",
                            "TIMESTAMP",
                            "NONE",
                        ]:
                            continue

                        minimums = (
                            group.column(column_idx).statistics.min
                            for group in iter_groups(metadata)
                        )
                        # If some row groups have all null values, their min and max will be null too.
                        stats["minValues"][name] = min(
                            minimum for minimum in minimums if minimum is not None
                        )
                        maximums = (
                            group.column(column_idx).statistics.max
                            for group in iter_groups(metadata)
                        )
                        stats["maxValues"][name] = max(
                            maximum for maximum in maximums if maximum is not None
                        )
        return stats

    def _get_current_delta_version(self, storage_options: Optional[dict] = None) -> int:
        """
        Fetch the current version of the Delta table, or -1 if new.

        Args:
            storage_options: Optional storage options to pass to ``try_get_deltatable``

        Returns:
            The integer version.
        """
        table = try_get_deltatable(self.delta_uri, storage_options=storage_options)
        if table is None:
            return -1
        else:
            table.update_incremental()
            return table.version()

    def write_raw_data(
        self,
        batch_ref: ObjectRef,
        file_options: Optional[Dict] = None,
        storage_options: Optional[Dict] = None,
    ) -> List[Any]:
        """
        Write a batch (Arrow Table, Dataset, etc.) to the Delta table.

        Args:
            batch_ref: The data to write
            file_options: File-level write options.
            storage_options: Optional storage options to pass to ``_get_current_delta_version``

        Returns:
            A list of AddAction objects for the new files.
        """
        add_actions = []

        # Get the latest version of the Delta table in case of concurrent writes
        version = self._get_current_delta_version(storage_options=storage_options)

        # Extract the base directory from the Delta URI
        # base_dir = urlparse(self.delta_uri).path.lstrip("/")

        parsed = urlparse(self.delta_uri)
        base_dir = f"{parsed.netloc}{parsed.path}"
        base_dir = base_dir.lstrip("/")

        # Generate a unique basename for the files to be written
        basename_template = f"{version + 1}-{uuid.uuid4()}-{{i}}.parquet"

        def visitor(written_file: Any) -> None:
            add_action = self._get_add_action_for_written_file(
                written_file, self.delta_uri
            )
            add_actions.append(add_action)

        arrow_table = BlockAccessor.for_block(batch_ref).to_arrow()

        # Validate Arrow table has data
        if len(arrow_table) == 0:
            return []  # No data to write

        # Ensure base_dir is absolute path for local filesystem
        if not base_dir.startswith("/") and not "://" in base_dir:
            base_dir = "/" + base_dir

        # Import call_with_retry for proper error handling like ParquetDatasource
        from ray.data._internal.util import call_with_retry
        from ray.data.context import DataContext

        def write_dataset_with_retry():
            return pa_ds.write_dataset(
                arrow_table,
                base_dir=base_dir,
                basename_template=basename_template,
                format="parquet",
                partitioning=self.partitioning,
                schema=self.schema,
                file_visitor=visitor,
                existing_data_behavior="overwrite_or_ignore",
                file_options=file_options,
                max_open_files=self.write_config.max_open_files,
                max_rows_per_file=self.write_config.max_rows_per_file,
                min_rows_per_group=self.write_config.min_rows_per_group,
                max_rows_per_group=self.write_config.max_rows_per_group,
                filesystem=self.filesystem,
                max_partitions=self.max_partitions,
            )

        call_with_retry(
            write_dataset_with_retry,
            description="write parquet dataset",
            match=DataContext.get_current().retried_io_errors,
        )

        return add_actions


def _validate_table_uri(table_uri: str) -> str:
    """Validate and normalize Delta table URI."""
    if not table_uri or not isinstance(table_uri, str):
        raise ValueError("table_uri must be a non-empty string")

    # Handle local paths
    if not table_uri.startswith(("s3://", "gs://", "abfss://", "file://", "/")):
        # Assume local path
        import os

        table_uri = os.path.abspath(table_uri)

    return table_uri


def _validate_positive_int(
    value: Optional[int], name: str, allow_none: bool = True
) -> None:
    """Validate positive integer parameter."""
    if value is None and allow_none:
        return
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer, got {value}")


def _validate_columns(
    columns: Optional[List[str]], name: str, allow_none: bool = True
) -> None:
    """Validate column list parameter."""
    if columns is None and allow_none:
        return
    if not isinstance(columns, list) or not all(
        isinstance(col, str) for col in columns
    ):
        raise ValueError(f"{name} must be a list of strings")
    if not columns:
        raise ValueError(f"{name} cannot be empty")


@ray.remote(num_cpus=1)
def _delete_file_task(file_path: str, filesystem) -> Dict[str, Any]:
    """Ray task to delete a single parquet file. Estimated size: 128 Mb"""
    try:
        filesystem.delete_file(file_path)
        return {"status": "success", "file": file_path}
    except Exception as e:
        return {"status": "error", "file": file_path, "error": str(e)}


def compact_delta_table(
    table_uri: str,
    *,
    target_size_bytes: Optional[int] = None,
    max_concurrent_tasks: Optional[int] = None,
    min_commit_interval: Optional[Union[int, timedelta]] = None,
    partition_filters: Optional[List[Tuple[str, str, Any]]] = None,
    filesystem: Optional[pa_fs.FileSystem] = None,
    storage_options: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Compact Delta table files for better performance using Ray parallelization.

    Args:
        table_uri: URI of the Delta table
        target_size_bytes: Target file size after compaction (default: 256MB)
        max_concurrent_tasks: Maximum concurrent compaction tasks
        min_commit_interval: Minimum interval between commits
        partition_filters: Partition filters for selective compaction
        filesystem: PyArrow filesystem (inferred if None)
        storage_options: Storage options for the filesystem

    Returns:
        Dictionary with compaction metrics

    Raises:
        ValueError: If arguments are invalid
        FileNotFoundError: If table doesn't exist
    """
    # Validate arguments
    table_uri = _validate_table_uri(table_uri)
    _validate_positive_int(target_size_bytes, "target_size_bytes")
    _validate_positive_int(max_concurrent_tasks, "max_concurrent_tasks")

    try:
        import deltalake as dl

        dt = dl.DeltaTable(table_uri, storage_options=storage_options)

        kwargs = {}
        if target_size_bytes:
            kwargs["target_size"] = target_size_bytes
        if max_concurrent_tasks:
            kwargs["max_concurrent_tasks"] = max_concurrent_tasks
        if min_commit_interval:
            kwargs["min_commit_interval"] = min_commit_interval
        if partition_filters:
            kwargs["partition_filters"] = partition_filters

        logger.info(f"Starting compaction of Delta table: {table_uri}")
        result = dt.optimize.compact(**kwargs)
        logger.info(f"Compaction completed: {result}")

        return result

    except Exception as e:
        logger.error(f"Compaction failed for {table_uri}: {e}")
        raise


def z_order_delta_table(
    table_uri: str,
    columns: List[str],
    *,
    target_size_bytes: Optional[int] = None,
    max_concurrent_tasks: Optional[int] = None,
    min_commit_interval: Optional[Union[int, timedelta]] = None,
    partition_filters: Optional[List[Tuple[str, str, Any]]] = None,
    max_spill_size: int = 21474836480,  # 20GB
    filesystem: Optional[pa_fs.FileSystem] = None,
    storage_options: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Optimize Delta table using Z-order for improved data skipping.

    Args:
        table_uri: URI of the Delta table
        columns: List of columns to use for Z-ordering
        target_size_bytes: Target file size after optimization
        max_concurrent_tasks: Maximum concurrent optimization tasks
        min_commit_interval: Minimum interval between commits
        partition_filters: Partition filters for selective optimization
        max_spill_size: Maximum spill size in bytes (default: 20GB)
        filesystem: PyArrow filesystem (inferred if None)
        storage_options: Storage options for the filesystem

    Returns:
        Dictionary with Z-order optimization metrics

    Raises:
        ValueError: If arguments are invalid
        FileNotFoundError: If table doesn't exist
    """
    # Validate arguments
    table_uri = _validate_table_uri(table_uri)
    _validate_columns(columns, "columns", allow_none=False)
    _validate_positive_int(target_size_bytes, "target_size_bytes")
    _validate_positive_int(max_concurrent_tasks, "max_concurrent_tasks")
    _validate_positive_int(max_spill_size, "max_spill_size", allow_none=False)

    try:
        import deltalake as dl

        dt = dl.DeltaTable(table_uri, storage_options=storage_options)

        kwargs = {"columns": columns, "max_spill_size": max_spill_size}
        if target_size_bytes:
            kwargs["target_size"] = target_size_bytes
        if max_concurrent_tasks:
            kwargs["max_concurrent_tasks"] = max_concurrent_tasks
        if min_commit_interval:
            kwargs["min_commit_interval"] = min_commit_interval
        if partition_filters:
            kwargs["partition_filters"] = partition_filters

        logger.info(
            f"Starting Z-order optimization of Delta table: {table_uri} on columns: {columns}"
        )
        result = dt.optimize.z_order(**kwargs)
        logger.info(f"Z-order optimization completed: {result}")

        return result

    except Exception as e:
        logger.error(f"Z-order optimization failed for {table_uri}: {e}")
        raise


def vacuum_delta_table(
    table_uri: str,
    *,
    retention_hours: Optional[int] = None,
    dry_run: bool = True,
    enforce_retention_duration: bool = True,
    parallel_deletion: bool = True,
    max_concurrent_deletes: int = 128,
    filesystem: Optional[pa_fs.FileSystem] = None,
    storage_options: Optional[Dict[str, str]] = None,
) -> List[str]:
    """
    Remove old files using scalable Ray parallelization for deletion.

    Args:
        table_uri: URI of the Delta table
        retention_hours: Retention threshold in hours
        dry_run: If True, only list files without deleting
        enforce_retention_duration: Whether to enforce minimum retention
        parallel_deletion: Use Ray tasks for parallel file deletion
        max_concurrent_deletes: Maximum concurrent delete tasks
        filesystem: PyArrow filesystem (inferred if None)
        storage_options: Storage options for the filesystem

    Returns:
        List of files that were deleted (or would be deleted in dry run)

    Raises:
        ValueError: If arguments are invalid
        FileNotFoundError: If table doesn't exist
    """
    # Validate arguments
    table_uri = _validate_table_uri(table_uri)
    _validate_positive_int(retention_hours, "retention_hours")
    _validate_positive_int(
        max_concurrent_deletes, "max_concurrent_deletes", allow_none=False
    )

    if not isinstance(dry_run, bool):
        raise ValueError("dry_run must be a boolean")
    if not isinstance(enforce_retention_duration, bool):
        raise ValueError("enforce_retention_duration must be a boolean")
    if not isinstance(parallel_deletion, bool):
        raise ValueError("parallel_deletion must be a boolean")

    try:
        import deltalake as dl

        dt = dl.DeltaTable(table_uri, storage_options=storage_options)

        # Get list of files to vacuum using Delta Lake's vacuum
        kwargs = {
            "dry_run": True,  # Always do dry run first to get file list
            "enforce_retention_duration": enforce_retention_duration,
        }
        if retention_hours:
            kwargs["retention_hours"] = retention_hours

        logger.info(f"Getting vacuum file list for Delta table: {table_uri}")
        files_to_delete = dt.vacuum(**kwargs)

        if dry_run:
            logger.info(f"Dry run: {len(files_to_delete)} files would be deleted")
            return files_to_delete

        if not files_to_delete:
            logger.info("No files to vacuum")
            return []

        logger.info(
            f"Vacuuming {len(files_to_delete)} files from Delta table: {table_uri}"
        )

        if parallel_deletion and len(files_to_delete) > 1:
            # Use Ray for parallel deletion
            logger.info(
                f"Using Ray parallel deletion with {max_concurrent_deletes} concurrent tasks"
            )

            # Get filesystem for deletion
            if filesystem is None:
                from ray.data.datasource.path_util import _resolve_paths_and_filesystem

                _, filesystem = _resolve_paths_and_filesystem([table_uri], None)

            # Split files into chunks for parallel processing
            chunk_size = max(1, len(files_to_delete) // max_concurrent_deletes)
            file_chunks = [
                files_to_delete[i : i + chunk_size]
                for i in range(0, len(files_to_delete), chunk_size)
            ]

            # Launch Ray tasks for deletion
            tasks = []
            for chunk in file_chunks:
                for file_path in chunk:
                    task = _delete_file_task.remote(file_path, filesystem)
                    tasks.append(task)

            # Wait for all deletions to complete
            results = ray.get(tasks)

            # Check results
            successful_deletes = []
            failed_deletes = []
            for result in results:
                if result["status"] == "success":
                    successful_deletes.append(result["file"])
                else:
                    failed_deletes.append(result)
                    logger.warning(
                        f"Failed to delete {result['file']}: {result['error']}"
                    )

            logger.info(
                f"Parallel vacuum completed: {len(successful_deletes)} deleted, {len(failed_deletes)} failed"
            )
            return successful_deletes
        else:
            # Use Delta Lake's built-in vacuum
            kwargs["dry_run"] = False
            deleted_files = dt.vacuum(**kwargs)
            logger.info(f"Vacuum completed: {len(deleted_files)} files deleted")
            return deleted_files

    except Exception as e:
        logger.error(f"Vacuum failed for {table_uri}: {e}")
        raise


class DeltaTableOptimizer:
    """Utility class for Delta table optimization operations."""

    def __init__(self, table_uri: str, filesystem: Optional[pa_fs.FileSystem] = None):
        """Initialize optimizer with table URI."""
        self.table_uri = table_uri
        self.filesystem = filesystem

    def optimize(self, config: OptimizationConfig) -> Dict[str, Any]:
        """Perform table optimization based on configuration."""
        import deltalake as dl

        try:
            dt = dl.DeltaTable(self.table_uri)

            if config.mode == OptimizationMode.COMPACT:
                return self._compact(dt, config)
            elif config.mode == OptimizationMode.Z_ORDER:
                return self._z_order(dt, config)
            elif config.mode == OptimizationMode.VACUUM:
                return self._vacuum(dt, config)
            else:
                raise ValueError(f"Unknown optimization mode: {config.mode}")

        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            raise

    def _compact(self, dt, config: OptimizationConfig) -> Dict[str, Any]:
        """Perform file compaction."""
        kwargs = {}
        if config.target_size_bytes:
            kwargs["target_size"] = config.target_size_bytes
        if config.max_concurrent_tasks:
            kwargs["max_concurrent_tasks"] = config.max_concurrent_tasks
        if config.min_commit_interval:
            kwargs["min_commit_interval"] = config.min_commit_interval
        if config.partition_filters:
            kwargs["partition_filters"] = config.partition_filters

        return dt.optimize.compact(**kwargs)

    def _z_order(self, dt, config: OptimizationConfig) -> Dict[str, Any]:
        """Perform Z-order optimization."""
        if not config.z_order_columns:
            raise ValueError(
                "Z-order columns must be specified for Z-order optimization"
            )

        kwargs = {
            "columns": config.z_order_columns,
            "max_spill_size": config.max_spill_size,
        }
        if config.target_size_bytes:
            kwargs["target_size"] = config.target_size_bytes
        if config.max_concurrent_tasks:
            kwargs["max_concurrent_tasks"] = config.max_concurrent_tasks
        if config.min_commit_interval:
            kwargs["min_commit_interval"] = config.min_commit_interval
        if config.partition_filters:
            kwargs["partition_filters"] = config.partition_filters

        return dt.optimize.z_order(**kwargs)

    def _vacuum(self, dt, config: OptimizationConfig) -> List[str]:
        """Perform vacuum operation."""
        kwargs = {"dry_run": False}  # Always perform actual vacuum in datasink integration
        if config.retention_hours:
            kwargs["retention_hours"] = config.retention_hours

        return dt.vacuum(**kwargs)


class DeltaTableMerger:
    """Utility class for Delta table merge operations."""

    def __init__(self, table_uri: str, filesystem: Optional[pa_fs.FileSystem] = None):
        """Initialize merger with table URI."""
        self.table_uri = table_uri
        self.filesystem = filesystem

    def merge_microbatch(
        self, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """
        Perform merge operation on a microbatch of data with configurable retry logic for concurrent writes.

        Args:
            source_data: Arrow table containing the source data to merge
            config: MergeConfig with retry settings and merge logic configuration

        Returns:
            Dictionary containing merge metrics and results

        Raises:
            ValueError: If merge configuration is invalid
            Exception: If merge operation fails after all retry attempts
        """
        import deltalake as dl
        import time
        import random

        # Use configurable retry parameters
        max_retries = config.max_retry_attempts
        base_delay = config.base_retry_delay
        max_delay = config.max_retry_delay

        for attempt in range(max_retries + 1):
            try:
                # Always get fresh table instance to get latest version
                dt = dl.DeltaTable(self.table_uri)

                # Handle SCD configuration
                if config.scd_config:
                    return self._execute_scd_merge(dt, source_data, config)

                # Handle Databricks-style merge conditions
                elif config.merge_conditions:
                    return self._execute_databricks_merge(dt, source_data, config)

                # Handle legacy mode-based merges
                elif config.mode == MergeMode.UPSERT:
                    return self._execute_upsert(dt, source_data, config)
                elif config.mode == MergeMode.INSERT_ONLY:
                    return self._execute_insert_only(dt, source_data, config)
                elif config.mode == MergeMode.UPDATE_ONLY:
                    return self._execute_update_only(dt, source_data, config)
                elif config.mode == MergeMode.DELETE:
                    return self._execute_delete(dt, source_data, config)
                else:
                    raise ValueError(f"Unknown merge mode: {config.mode}")

            except Exception as e:
                # Check if this is a commit conflict that we can retry
                error_str = str(e).lower()
                error_type_str = str(type(e)).lower()

                is_retryable_conflict = (
                    "commitfailederror" in error_type_str
                    or "concurrent" in error_str
                    or "conflict" in error_str
                    or "version mismatch" in error_str
                    or "optimistic concurrency" in error_str
                )

                if is_retryable_conflict and attempt < max_retries:
                    # Calculate exponential backoff with jitter, capped at max_delay
                    exponential_delay = base_delay * (2 ** attempt)
                    jitter = random.uniform(0, base_delay * 0.1)
                    delay = min(exponential_delay + jitter, max_delay)

                    logger.warning(
                        f"Delta Lake merge conflict detected - this is normal in concurrent environments. "
                        f"Retrying operation (attempt {attempt + 1}/{max_retries + 1}) after {delay:.2f}s delay. "
                        f"Conflict type: {type(e).__name__}. "
                        f"To reduce conflicts, consider: 1) Using fewer concurrent writers, "
                        f"2) Partitioning data by different keys, 3) Writing larger batches less frequently. "
                        f"Original error: {e}"
                    )
                    time.sleep(delay)
                    continue
                elif is_retryable_conflict:
                    # Exhausted all retries for a retryable error
                    logger.error(
                        f"Delta Lake merge operation failed after {max_retries + 1} attempts due to persistent conflicts. "
                        f"This indicates high contention on the table. Consider: "
                        f"1) Reducing concurrent writers, 2) Using different partition keys, "
                        f"3) Increasing retry attempts with config.max_retry_attempts, "
                        f"4) Writing data in larger, less frequent batches. "
                        f"Final error: {e}"
                    )
                else:
                    # Non-retryable error
                    logger.error(
                        f"Delta Lake merge operation failed with non-retryable error: {type(e).__name__}: {e}. "
                        f"This error cannot be resolved by retrying and indicates a configuration or data issue."
                    )

                raise

    def _execute_scd_merge(
        self, dt, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Execute SCD (Slowly Changing Dimensions) merge based on scd_type."""
        scd_config = config.scd_config

        if scd_config.scd_type == 1:
            return self._execute_scd_type_1(dt, source_data, config)
        elif scd_config.scd_type == 2:
            return self._execute_scd_type_2(dt, source_data, config)
        elif scd_config.scd_type == 3:
            return self._execute_scd_type_3(dt, source_data, config)
        else:
            raise ValueError(f"Unsupported SCD type: {scd_config.scd_type}")

    def _execute_scd_type_1(
        self, dt, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Execute SCD Type 1 (overwrite changes)."""
        scd_config = config.scd_config

        # Build predicate from key columns
        key_conditions = [
            f"{config.target_alias}.{col} = {config.source_alias}.{col}"
            for col in scd_config.key_columns
        ]
        predicate = " AND ".join(key_conditions)

        merger = dt.merge(
            source=source_data,
            predicate=predicate,
            source_alias=config.source_alias,
            target_alias=config.target_alias,
            error_on_type_mismatch=config.error_on_type_mismatch,
        )

        # SCD Type 1: Update existing records, insert new ones
        if scd_config.overwrite_columns:
            # Only update specified columns
            updates = {
                col: f"{config.source_alias}.{col}"
                for col in scd_config.overwrite_columns
            }
            merger = merger.when_matched_update(updates=updates)
        else:
            # Update all columns
            merger = merger.when_matched_update_all()

        merger = merger.when_not_matched_insert_all()

        return merger.execute()

    def _execute_scd_type_2(
        self, dt, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Execute SCD Type 2 (versioning with history)."""
        scd_config = config.scd_config
        current_time = datetime.now()

        # Build key predicate
        key_conditions = [
            f"{config.target_alias}.{col} = {config.source_alias}.{col}"
            for col in scd_config.key_columns
        ]
        key_predicate = " AND ".join(key_conditions)

        # Build change detection predicate
        if scd_config.hash_columns:
            change_conditions = [
                f"{config.target_alias}.{col} != {config.source_alias}.{col}"
                for col in scd_config.hash_columns
            ]
            change_predicate = f"{key_predicate} AND ({' OR '.join(change_conditions)}) AND {config.target_alias}.{scd_config.current_flag_column} = true"
        else:
            change_predicate = f"{key_predicate} AND {config.target_alias}.{scd_config.current_flag_column} = true"

        # Step 1: Expire current records that have changes
        expire_updates = {
            scd_config.current_flag_column: "false",
            scd_config.end_time_column: f"'{current_time.isoformat()}'",
        }

        merger = dt.merge(
            source=source_data,
            predicate=change_predicate,
            source_alias=config.source_alias,
            target_alias=config.target_alias,
            error_on_type_mismatch=config.error_on_type_mismatch,
        )
        merger = merger.when_matched_update(updates=expire_updates)
        expire_result = merger.execute()

        # Step 2: Insert new versions
        insert_updates = {}
        for col in source_data.schema.names:
            insert_updates[col] = f"{config.source_alias}.{col}"

        # Add SCD Type 2 metadata
        insert_updates[scd_config.current_flag_column] = "true"
        insert_updates[scd_config.start_time_column] = f"'{current_time.isoformat()}'"
        insert_updates[scd_config.end_time_column] = "null"

        merger = dt.merge(
            source=source_data,
            predicate=key_predicate,
            source_alias=config.source_alias,
            target_alias=config.target_alias,
            error_on_type_mismatch=config.error_on_type_mismatch,
        )
        merger = merger.when_not_matched_insert(updates=insert_updates)
        insert_result = merger.execute()

        # Combine results
        combined_result = expire_result.copy()
        for key, value in insert_result.items():
            if isinstance(value, (int, float)):
                combined_result[key] = combined_result.get(key, 0) + value
            else:
                combined_result[f"insert_{key}"] = value

        return combined_result

    def _execute_scd_type_3(
        self, dt, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Execute SCD Type 3 (store previous values in additional columns)."""
        scd_config = config.scd_config

        if not scd_config.change_columns:
            raise ValueError("change_columns required for SCD Type 3")

        # Build predicate from key columns
        key_conditions = [
            f"{config.target_alias}.{col} = {config.source_alias}.{col}"
            for col in scd_config.key_columns
        ]
        predicate = " AND ".join(key_conditions)

        # Build change detection predicate
        change_conditions = [
            f"{config.target_alias}.{col} != {config.source_alias}.{col}"
            for col in scd_config.change_columns
        ]
        change_predicate = f"{predicate} AND ({' OR '.join(change_conditions)})"

        merger = dt.merge(
            source=source_data,
            predicate=predicate,
            source_alias=config.source_alias,
            target_alias=config.target_alias,
            error_on_type_mismatch=config.error_on_type_mismatch,
        )

        # SCD Type 3: Update current values and store previous values
        updates = {}
        for col in source_data.schema.names:
            if col in scd_config.change_columns:
                # Store previous value and update current
                prev_col = f"{col}{scd_config.previous_value_suffix}"
                updates[
                    prev_col
                ] = f"{config.target_alias}.{col}"  # Store current as previous
                updates[col] = f"{config.source_alias}.{col}"  # Update with new value
            elif col not in scd_config.key_columns:
                # Update non-key columns normally
                updates[col] = f"{config.source_alias}.{col}"

        merger = merger.when_matched_update(updates=updates, predicate=change_predicate)
        merger = merger.when_not_matched_insert_all()

        return merger.execute()

    def _execute_databricks_merge(
        self, dt, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Execute Databricks-style merge with complex conditions."""
        conditions = config.merge_conditions

        merger = dt.merge(
            source=source_data,
            predicate=conditions.merge_predicate,
            source_alias=config.source_alias,
            target_alias=config.target_alias,
            error_on_type_mismatch=config.error_on_type_mismatch,
        )

        # When matched operations
        if conditions.when_matched_update:
            merger = merger.when_matched_update(
                updates=conditions.when_matched_update,
                predicate=conditions.when_matched_update_condition,
            )

        if conditions.when_matched_delete:
            merger = merger.when_matched_delete(
                predicate=conditions.when_matched_delete_condition
            )

        # When not matched operations
        if conditions.when_not_matched_insert:
            merger = merger.when_not_matched_insert(
                updates=conditions.when_not_matched_insert,
                predicate=conditions.when_not_matched_insert_condition,
            )

        # When not matched by source operations
        if conditions.when_not_matched_by_source_update:
            merger = merger.when_not_matched_by_source_update(
                updates=conditions.when_not_matched_by_source_update,
                predicate=conditions.when_not_matched_by_source_update_condition,
            )

        if conditions.when_not_matched_by_source_delete:
            merger = merger.when_not_matched_by_source_delete(
                predicate=conditions.when_not_matched_by_source_delete_condition
            )

        return merger.execute()

    def _execute_upsert(
        self, dt, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Perform upsert operation."""
        if not config.predicate:
            raise ValueError("Predicate is required for upsert operations")

        merger = dt.merge(
            source=source_data,
            predicate=config.predicate,
            source_alias=config.source_alias,
            target_alias=config.target_alias,
            error_on_type_mismatch=config.error_on_type_mismatch,
        )

        # Configure update behavior
        if config.update_columns:
            merger = merger.when_matched_update(updates=config.update_columns)
        else:
            merger = merger.when_matched_update_all()

        # Configure insert behavior
        if config.insert_columns:
            merger = merger.when_not_matched_insert(updates=config.insert_columns)
        else:
            merger = merger.when_not_matched_insert_all()

        return merger.execute()

    def _execute_insert_only(
        self, dt, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Perform insert-only operation."""
        if not config.predicate:
            raise ValueError("Predicate is required for insert-only operations")

        merger = dt.merge(
            source=source_data,
            predicate=config.predicate,
            source_alias=config.source_alias,
            target_alias=config.target_alias,
            error_on_type_mismatch=config.error_on_type_mismatch,
        )

        if config.insert_columns:
            merger = merger.when_not_matched_insert(updates=config.insert_columns)
        else:
            merger = merger.when_not_matched_insert_all()

        return merger.execute()

    def _execute_update_only(
        self, dt, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Perform update-only operation."""
        if not config.predicate:
            raise ValueError("Predicate is required for update-only operations")

        merger = dt.merge(
            source=source_data,
            predicate=config.predicate,
            source_alias=config.source_alias,
            target_alias=config.target_alias,
            error_on_type_mismatch=config.error_on_type_mismatch,
        )

        if config.update_columns:
            merger = merger.when_matched_update(updates=config.update_columns)
        else:
            merger = merger.when_matched_update_all()

        return merger.execute()

    def _execute_delete(
        self, dt, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Perform delete operation."""
        if not config.predicate:
            raise ValueError("Predicate is required for delete operations")

        merger = dt.merge(
            source=source_data,
            predicate=config.predicate,
            source_alias=config.source_alias,
            target_alias=config.target_alias,
            error_on_type_mismatch=config.error_on_type_mismatch,
        )

        if config.delete_predicate:
            merger = merger.when_matched_delete(predicate=config.delete_predicate)
        else:
            merger = merger.when_matched_delete()

        return merger.execute()


class DeltaDatasink(_FileDatasink):

    NAME = "Delta"

    def __init__(
        self,
        path: str,
        *,
        mode: str = WriteMode.APPEND.value,
        filesystem: Optional[pa_fs.FileSystem] = None,
        delta_config: Optional[Union[DeltaWriteConfig, dict]] = None,
        merge_config: Optional[Union[MergeConfig, dict]] = None,
        merge_conditions: Optional[MergeConditions] = None,
        scd_config: Optional[SCDConfig] = None,
        scd_type: Optional[int] = None,
        key_columns: Optional[List[str]] = None,
        target_columns: Optional[List[str]] = None,
        optimization_config: Optional[Union[OptimizationConfig, dict]] = None,
        enable_optimization: bool = False,
        enable_microbatch_writes: bool = False,
        microbatch_size: Optional[int] = None,
        upsert_condition: Optional[str] = None,
        insert_condition: Optional[str] = None,
        update_condition: Optional[str] = None,
        delete_condition: Optional[str] = None,
        **file_datasink_kwargs,
    ):
        """
        Ray Data datasink for writing blocks to a Delta Lake table.

        This datasink provides comprehensive Delta Lake functionality including:
        - Standard write modes (append, overwrite, error, ignore)
        - Advanced merge operations with Databricks-style syntax
        - SCD (Slowly Changing Dimensions) Types 1, 2, and 3
        - Scalable microbatch processing
        - Table optimization features with automatic post-write optimization
        - ACID compliance and transaction safety
        - Multi-cloud support with automatic credential detection

        Supported Storage Systems:
        - Local filesystem: /path/to/table or file:///path/to/table
        - AWS S3: s3://bucket/path/to/table or s3a://bucket/path/to/table
        - Google Cloud Storage: gs://bucket/path/to/table or gcs://bucket/path/to/table  
        - Azure Blob Storage/ADLS: abfss://container@account.dfs.core.windows.net/path
        - Azure Data Lake: abfs://container@account.dfs.core.windows.net/path
        - Azure Classic: wasb://container@account.blob.core.windows.net/path
        - HDFS: hdfs://namenode:port/path/to/table

        Automatic Credential Detection:
        - AWS: Uses boto3 session credentials, IAM roles, or environment variables
        - GCP: Uses Application Default Credentials, service account keys, or environment variables
        - Azure: Uses DefaultAzureCredential, managed identity, or environment variables

        Args:
            path: URI or local filesystem path to the root of the Delta Lake table.
                Must be a valid string path with appropriate scheme for cloud storage.
            mode: Write behavior mode. Must be one of:
                - "append" (default): Add data to existing table
                - "overwrite": Replace entire table contents
                - "error": Fail if table already exists
                - "ignore": Skip write if table already exists
                - "merge": Perform merge operation using merge_config
            filesystem: Arrow-compatible filesystem object. If None, inferred from path.
            delta_config: Delta writer configuration as DeltaWriteConfig dataclass or dict.
            merge_config: Comprehensive merge configuration supporting:
                - Simple upsert operations
                - SCD configurations with scd_type parameter
                - Databricks-style merge conditions
                - Complex multi-condition merge patterns
                - Configurable retry logic for concurrent write conflicts
            optimization_config: Table optimization configuration for compaction, Z-order, vacuum.
            enable_optimization: If True, automatically run optimization after write operations
                using the optimization_config settings. Defaults to False.
            enable_microbatch_writes: Enable microbatch processing for memory efficiency.
            microbatch_size: Size of each microbatch (default: auto-calculated).
            scd_type: Convenience parameter for SCD operations (1, 2, or 3).
                Automatically configures merge_config for the specified SCD type.
            key_columns: Business key columns for SCD operations (required if scd_type specified).
            **file_datasink_kwargs: Additional file datasink options.

        Raises:
            ValueError: If arguments are invalid or incompatible
            TypeError: If arguments are wrong type

        Examples:
            Basic append to AWS S3:
            >>> datasink = DeltaDatasink(
            ...     path="s3://bucket/my-table",
            ...     delta_config={"engine": "rust", "max_open_files": 128}
            ... )

            Basic append to Google Cloud Storage:
            >>> datasink = DeltaDatasink(
            ...     path="gs://bucket/my-table",
            ...     delta_config={"engine": "rust"}
            ... )

            Basic append to Azure Data Lake Storage:
            >>> datasink = DeltaDatasink(
            ...     path="abfss://container@account.dfs.core.windows.net/my-table",
            ...     delta_config={"engine": "rust"}
            ... )

            Simple SCD Type 2 using convenience parameters:
            >>> datasink = DeltaDatasink(
            ...     path="./warehouse/customers",
            ...     mode="merge",
            ...     scd_type=2,
            ...     key_columns=["customer_id"],
            ...     enable_microbatch_writes=True
            ... )

            Advanced Merges with Custom Retry Configuration:
            >>> merge_conditions = MergeConditions(
            ...     merge_predicate="target.id = source.id",
            ...     when_matched_update={"name": "source.name", "updated_at": "current_timestamp()"},
            ...     when_matched_update_condition="source.version > target.version",
            ...     when_not_matched_insert={"id": "source.id", "name": "source.name"},
            ...     when_not_matched_by_source_delete=True,
            ...     when_not_matched_by_source_delete_condition="target.is_active = true"
            ... )
            >>> merge_config = MergeConfig(
            ...     merge_conditions=merge_conditions,
            ...     max_retry_attempts=5,  # Increase retries for high-concurrency environments
            ...     base_retry_delay=0.2,  # Slower retry cadence
            ...     max_retry_delay=10.0   # Allow longer delays if needed
            ... )
            >>> datasink = DeltaDatasink(
            ...     path="s3://bucket/complex-table",
            ...     mode="merge",
            ...     merge_config=merge_config
            ... )

            Automatic Table Optimization:
            >>> optimization_config = OptimizationConfig(
            ...     mode=OptimizationMode.COMPACT,
            ...     target_size_bytes=128*1024*1024,  # 128MB target files
            ...     max_concurrent_tasks=4
            ... )
            >>> datasink = DeltaDatasink(
            ...     path="s3://bucket/optimized-table",
            ...     optimization_config=optimization_config,
            ...     enable_optimization=True  # Automatically optimize after writes
            ... )
        """
        # Comprehensive argument validation
        self._validate_arguments(
            path=path,
            mode=mode,
            filesystem=filesystem,
            delta_config=delta_config,
            merge_config=merge_config,
            merge_conditions=merge_conditions,
            scd_config=scd_config,
            scd_type=scd_type,
            key_columns=key_columns,
            target_columns=target_columns,
            optimization_config=optimization_config,
            enable_optimization=enable_optimization,
            enable_microbatch_writes=enable_microbatch_writes,
            microbatch_size=microbatch_size,
            upsert_condition=upsert_condition,
            insert_condition=insert_condition,
            update_condition=update_condition,
            delete_condition=delete_condition,
        )

        super().__init__(path, **file_datasink_kwargs)
        _check_import(self, module="deltalake", package="deltalake")

        # If config is a dict, convert to DeltaWriteConfig; otherwise use as is or default.
        if delta_config is None:
            self.config = DeltaWriteConfig()
        elif isinstance(delta_config, dict):
            self.config = DeltaWriteConfig.from_dict(delta_config)
        elif isinstance(delta_config, DeltaWriteConfig):
            self.config = delta_config
        else:
            raise ValueError(
                "delta_config must be a DeltaWriteConfig instance or a dict"
            )

        # Set basic attributes first
        self.path = path
        self.mode = mode
        self.enable_optimization = enable_optimization
        self.enable_microbatch_writes = enable_microbatch_writes
        self.microbatch_size = microbatch_size or self._get_default_microbatch_size()

        # These can be dynamically set/discovered at runtime, so keep direct attribute.
        self.schema = self.config.schema
        self.partition_cols = self.config.partition_cols
        if isinstance(self.partition_cols, str):
            self.partition_cols = [self.partition_cols]

        # Handle merge configuration (including SCD convenience parameters)
        self.merge_config = self._configure_merge_config(
            merge_config, scd_type, key_columns
        )

        # Validate merge configuration for merge mode
        if (
            self.mode == WriteMode.MERGE.value
            and not self.merge_config.scd_config
            and not self.merge_config.merge_conditions
            and not self.merge_config.predicate
        ):
            raise ValueError(
                "Must specify either scd_config, merge_conditions, or predicate for merge operations"
            )

        # Handle optimization configuration
        if optimization_config is None:
            self.optimization_config = OptimizationConfig()
        elif isinstance(optimization_config, dict):
            self.optimization_config = OptimizationConfig(**optimization_config)
        elif isinstance(optimization_config, OptimizationConfig):
            self.optimization_config = optimization_config
        else:
            raise ValueError(
                "optimization_config must be an OptimizationConfig instance or a dict"
            )
        if filesystem is not None:
            self.filesystem = filesystem

        # Detect cloud provider from path scheme
        path_lower = self.path.lower()
        self.is_aws = path_lower.startswith(("s3://", "s3a://"))
        self.is_gcp = path_lower.startswith(("gs://", "gcs://"))
        self.is_azure = path_lower.startswith(("abfss://", "abfs://", "adl://", "wasb://", "wasbs://"))
        self.storage_options = self._get_storage_options()

        # Initialize utility classes
        self._optimizer = DeltaTableOptimizer(self.path, filesystem)
        self._merger = DeltaTableMerger(self.path, filesystem)

    def _validate_arguments(
        self,
        *,
        path: str,
        mode: str,
        filesystem,
        delta_config,
        merge_config,
        merge_conditions: Optional[MergeConditions],
        scd_config: Optional[SCDConfig],
        scd_type: Optional[int],
        key_columns: Optional[List[str]],
        target_columns: Optional[List[str]],
        optimization_config,
        enable_optimization: bool,
        enable_microbatch_writes: bool,
        microbatch_size: Optional[int],
        upsert_condition: Optional[str],
        insert_condition: Optional[str],
        update_condition: Optional[str],
        delete_condition: Optional[str],
    ) -> None:
        """
        Comprehensive argument validation for DeltaDatasink.

        Validates all input parameters for type correctness, value ranges,
        logical consistency, and compatibility between different options.

        Raises:
            ValueError: For invalid values or incompatible parameter combinations
            TypeError: For incorrect parameter types
        """
        # ===== PATH VALIDATION =====
        if not isinstance(path, str):
            raise TypeError(f"path must be a string, got {type(path).__name__}")

        if not path.strip():
            raise ValueError("path cannot be empty or whitespace-only")

        # Validate path format for known schemes
        path_lower = path.lower()
        supported_schemes = [
            "file://",
            "local://",
            "s3://",
            "s3a://",
            "gs://",
            "gcs://",
            "abfss://",
            "abfs://",
            "adl://",
            "wasb://",
            "wasbs://",
            "hdfs://",
        ]
        cloud_schemes = ("s3://", "s3a://", "gs://", "gcs://", "abfss://", "abfs://", "adl://", "wasb://", "wasbs://")

        if any(path_lower.startswith(scheme) for scheme in supported_schemes):
            if (
                path_lower.startswith(cloud_schemes)
                and len(path.split("/")) < 4
            ):
                raise ValueError(f"Cloud storage path appears incomplete: {path}")

        # Provide helpful scheme suggestions for common mistakes
        if path_lower.startswith("azure://"):
            raise ValueError("Use 'abfss://' or 'abfs://' instead of 'azure://' for Azure paths")
        elif path_lower.startswith("gcp://") or path_lower.startswith("google://"):
            raise ValueError("Use 'gs://' instead of 'gcp://' or 'google://' for Google Cloud paths")

        # Check for suspicious characters that might cause issues
        invalid_chars = ["*", "?", "<", ">", "|"]
        if any(char in path for char in invalid_chars):
            raise ValueError(
                f"path contains invalid characters {invalid_chars}: {path}"
            )

        if not isinstance(mode, str):
            raise TypeError(f"mode must be a string, got {type(mode).__name__}")

        valid_modes = [m.value for m in WriteMode]
        if mode not in valid_modes:
            available_modes = "', '".join(valid_modes)
            raise ValueError(f"mode must be one of ['{available_modes}'], got '{mode}'")

        if filesystem is not None:
            # Basic duck-typing check for filesystem interface
            required_methods = ["get_file_info", "open_output_stream"]
            for method in required_methods:
                if not hasattr(filesystem, method):
                    raise TypeError(f"filesystem must implement {method} method")

        if not isinstance(enable_microbatch_writes, bool):
            raise TypeError(
                f"enable_microbatch_writes must be a boolean, got {type(enable_microbatch_writes).__name__}"
            )

        if microbatch_size is not None:
            if not isinstance(microbatch_size, int):
                raise TypeError(
                    f"microbatch_size must be an integer, got {type(microbatch_size).__name__}"
                )

            if microbatch_size <= 0:
                raise ValueError(
                    f"microbatch_size must be positive, got {microbatch_size}"
                )

            if microbatch_size > 10_000_000:  # 10M rows
                raise ValueError(
                    f"microbatch_size too large (max 10M), got {microbatch_size}"
                )

            if microbatch_size < 100:
                import warnings

                warnings.warn(
                    f"microbatch_size ({microbatch_size}) is very small and may hurt performance. "
                    "Consider using at least 1000 rows per batch.",
                    UserWarning,
                )


        def validate_column_list(columns, param_name):
            """Helper to validate column name lists."""
            if columns is not None:
                if not isinstance(columns, (list, tuple)):
                    raise TypeError(
                        f"{param_name} must be a list or tuple, got {type(columns).__name__}"
                    )

                if len(columns) == 0:
                    raise ValueError(f"{param_name} cannot be empty")

                if not all(isinstance(col, str) for col in columns):
                    invalid_types = [
                        type(col).__name__
                        for col in columns
                        if not isinstance(col, str)
                    ]
                    raise TypeError(
                        f"All {param_name} must be strings, found types: {set(invalid_types)}"
                    )

                if not all(col.strip() for col in columns):
                    raise ValueError(
                        f"{param_name} cannot contain empty or whitespace-only strings"
                    )

                # Check for duplicates
                if len(set(columns)) != len(columns):
                    duplicates = [col for col in set(columns) if columns.count(col) > 1]
                    raise ValueError(f"Duplicate columns in {param_name}: {duplicates}")

        validate_column_list(key_columns, "key_columns")
        validate_column_list(target_columns, "target_columns")

        if scd_type is not None:
            if not isinstance(scd_type, int):
                raise TypeError(
                    f"scd_type must be an integer, got {type(scd_type).__name__}"
                )

            if scd_type not in [1, 2, 3]:
                raise ValueError(f"scd_type must be 1, 2, or 3, got {scd_type}")

            if mode != WriteMode.MERGE.value:
                raise ValueError(
                    f"scd_type can only be used with mode='merge', got mode='{mode}'"
                )

            # Validate key columns when SCD type is specified
            if not key_columns:
                raise ValueError("key_columns are required when scd_type is specified")

            # Check for reserved column name conflicts
            reserved_scd_columns = {
                1: [],  # Type 1 has no reserved columns
                2: ["version", "is_current", "start_time", "end_time"],
                3: [],  # Type 3 creates _previous columns dynamically
            }

            conflicts = set(key_columns) & set(reserved_scd_columns[scd_type])
            if conflicts:
                raise ValueError(
                    f"key_columns conflict with reserved SCD Type {scd_type} columns: {conflicts}. "
                    f"Reserved columns: {reserved_scd_columns[scd_type]}"
                )

            # Prevent specifying both SCD and explicit configurations
            conflicting_configs = []
            if merge_config is not None:
                conflicting_configs.append("merge_config")
            if merge_conditions is not None:
                conflicting_configs.append("merge_conditions")
            if scd_config is not None:
                conflicting_configs.append("scd_config")

            if conflicting_configs:
                raise ValueError(
                    f"Cannot specify both scd_type and {conflicting_configs}. "
                    f"Use scd_type for simple SCD operations or explicit configs for advanced use cases."
                )

        # Validate key columns without SCD type
        elif key_columns is not None:
            # Key columns without SCD type might be used for simple merges, so just warn if no merge config
            if not any([merge_config, merge_conditions, upsert_condition]):
                import warnings

                warnings.warn(
                    "key_columns specified without merge configuration. "
                    "Specify merge_config, merge_conditions, or upsert_condition to use key_columns.",
                    UserWarning,
                )

        if merge_conditions is not None:
            if not isinstance(merge_conditions, MergeConditions):
                raise TypeError(
                    f"merge_conditions must be a MergeConditions instance, got {type(merge_conditions).__name__}"
                )

            # Prevent conflicting merge specifications
            if any([scd_type, scd_config, upsert_condition]):
                raise ValueError(
                    "Cannot specify merge_conditions with scd_type, scd_config, or simple merge conditions"
                )

        if scd_config is not None:
            if not isinstance(scd_config, SCDConfig):
                raise TypeError(
                    f"scd_config must be an SCDConfig instance, got {type(scd_config).__name__}"
                )

            if mode != WriteMode.MERGE.value:
                raise ValueError(
                    f"scd_config can only be used with mode='merge', got mode='{mode}'"
                )

            # Prevent conflicting merge specifications
            if any([scd_type, merge_config, merge_conditions, upsert_condition]):
                raise ValueError(
                    "Cannot specify scd_config with other merge configuration options"
                )

        def validate_condition_string(condition, param_name):
            """Helper to validate SQL condition strings."""
            if condition is not None:
                if not isinstance(condition, str):
                    raise TypeError(
                        f"{param_name} must be a string, got {type(condition).__name__}"
                    )

                if not condition.strip():
                    raise ValueError(f"{param_name} cannot be empty or whitespace-only")

                # Basic SQL injection protection (simple checks)
                dangerous_patterns = [";", "--", "/*", "*/", "xp_", "sp_"]
                condition_lower = condition.lower()
                for pattern in dangerous_patterns:
                    if pattern in condition_lower:
                        import warnings

                        warnings.warn(
                            f"{param_name} contains potentially dangerous pattern '{pattern}'. "
                            "Ensure your condition is safe and properly escaped.",
                            UserWarning,
                        )
                        break

        validate_condition_string(upsert_condition, "upsert_condition")
        validate_condition_string(insert_condition, "insert_condition")
        validate_condition_string(update_condition, "update_condition")
        validate_condition_string(delete_condition, "delete_condition")

        # Validate logical consistency of simple merge conditions
        if any(
            [upsert_condition, insert_condition, update_condition, delete_condition]
        ):
            if mode != WriteMode.MERGE.value:
                raise ValueError("Merge conditions can only be used with mode='merge'")

            # Prevent conflicting merge specifications
            if any([scd_type, scd_config, merge_config, merge_conditions]):
                raise ValueError(
                    "Cannot specify simple merge conditions with other merge configuration options"
                )

        if not isinstance(enable_optimization, bool):
            raise TypeError(
                f"enable_optimization must be a boolean, got {type(enable_optimization).__name__}"
            )

        if delta_config is not None:
            if not isinstance(delta_config, (dict, DeltaWriteConfig)):
                raise TypeError(
                    f"delta_config must be a dict or DeltaWriteConfig instance, got {type(delta_config).__name__}"
                )

            # Validate dict keys if it's a dict
            if isinstance(delta_config, dict):
                valid_config_keys = set(DeltaWriteConfig.__dataclass_fields__.keys())
                invalid_keys = set(delta_config.keys()) - valid_config_keys
                if invalid_keys:
                    raise ValueError(f"Invalid keys in delta_config: {invalid_keys}")

        if merge_config is not None:
            if not isinstance(merge_config, (dict, MergeConfig)):
                raise TypeError(
                    f"merge_config must be a dict or MergeConfig instance, got {type(merge_config).__name__}"
                )

            # Validate dict keys if it's a dict
            if isinstance(merge_config, dict):
                valid_config_keys = set(MergeConfig.__dataclass_fields__.keys())
                invalid_keys = set(merge_config.keys()) - valid_config_keys
                if invalid_keys:
                    raise ValueError(f"Invalid keys in merge_config: {invalid_keys}")

        if optimization_config is not None:
            if not isinstance(optimization_config, (dict, OptimizationConfig)):
                raise TypeError(
                    f"optimization_config must be a dict or OptimizationConfig instance, got {type(optimization_config).__name__}"
                )

        # Validate dict keys if it's a dict
        if isinstance(optimization_config, dict):
            valid_config_keys = set(OptimizationConfig.__dataclass_fields__.keys())
            # Remove dry_run from validation since it's not supported in datasink integration
            optimization_config = {k: v for k, v in optimization_config.items() if k != "dry_run"}
            invalid_keys = set(optimization_config.keys()) - valid_config_keys
            if invalid_keys:
                raise ValueError(
                    f"Invalid keys in optimization_config: {invalid_keys}"
                )

        if mode == WriteMode.MERGE.value:
            has_merge_spec = any(
                [
                    scd_type is not None,
                    merge_config is not None,
                    # Could also check for legacy merge parameters here
                ]
            )
            if not has_merge_spec:
                raise ValueError(
                    "Merge mode requires merge specification. Provide one of: "
                    "scd_type, merge_config, merge_conditions, or upsert_condition"
                )

        elif mode in [WriteMode.APPEND.value, WriteMode.OVERWRITE.value]:
            # These modes don't use merge configurations
            if scd_type is not None:
                raise ValueError(
                    f"scd_type cannot be used with mode='{mode}', only with mode='merge'"
                )

            if merge_config is not None:
                import warnings

                warnings.warn(
                    f"merge_config specified with mode='{mode}' will be ignored. "
                    "merge_config only applies to mode='merge'.",
                    UserWarning,
                )

        # Check microbatch settings consistency
        if not enable_microbatch_writes and microbatch_size is not None:
            import warnings

            warnings.warn(
                "microbatch_size specified but enable_microbatch_writes=False. "
                "microbatch_size will be ignored.",
                UserWarning,
            )

    def _configure_merge_config(
        self,
        merge_config: Optional[Union[MergeConfig, dict]],
        scd_type: Optional[int],
        key_columns: Optional[List[str]],
    ) -> MergeConfig:
        """Configure merge configuration, handling SCD convenience parameters."""

        # Handle SCD convenience parameters
        if scd_type is not None:
            scd_config = SCDConfig(scd_type=scd_type, key_columns=key_columns)
            return MergeConfig(scd_config=scd_config)

        # Handle explicit merge configuration
        if merge_config is None:
            return MergeConfig()
        elif isinstance(merge_config, dict):
            return MergeConfig(**merge_config)
        elif isinstance(merge_config, MergeConfig):
            return merge_config
        else:
            # This should not happen due to validation, but just in case
            raise ValueError("merge_config must be a MergeConfig instance or a dict")

    def _get_storage_options(self):
        """
        Get storage options for the appropriate cloud provider.

        Returns:
            Dictionary of storage options for Delta Lake
        """
        _storage_options = (
            {}
            if self.config.storage_options is None
            else self.config.storage_options.copy()
        )

        if self.is_aws:
            # AWS S3 configuration
            try:
                credentials = AWSUtilities._get_aws_credentials()
                region = AWSUtilities._get_aws_region()

                if credentials:
                    if credentials.access_key:
                        _storage_options["AWS_ACCESS_KEY_ID"] = credentials.access_key
                    if credentials.secret_key:
                        _storage_options["AWS_SECRET_ACCESS_KEY"] = credentials.secret_key
                    if credentials.token:
                        _storage_options["AWS_SESSION_TOKEN"] = credentials.token

                if region:
                    _storage_options["AWS_REGION"] = region

                # S3-specific Delta Lake options
                _storage_options["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"

            except Exception as e:
                logger.warning(
                    f"Failed to get AWS credentials: {e}. "
                    f"This is expected if accessing public S3 data or using explicit storage_options. "
                    f"For private S3 resources, ensure you have: 1) Configured AWS CLI with 'aws configure', "
                    f"2) Set AWS environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY), "
                    f"3) Assigned IAM roles to your EC2/Lambda instance, or 4) Use AWS SSO."
                )

        elif self.is_gcp:
            # Google Cloud Storage configuration
            try:
                credentials, project_id = GCPUtilities._get_gcp_credentials()

                if credentials:
                    # For GCS, Delta Lake typically uses service account JSON or OAuth2
                    # Check if we have a service account key
                    import os
                    service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                    if service_account_path:
                        _storage_options["GOOGLE_SERVICE_ACCOUNT"] = service_account_path
                    else:
                        # Use OAuth2 token if available
                        try:
                            import google.auth.transport.requests
                            request = google.auth.transport.requests.Request()
                            credentials.refresh(request)
                            if hasattr(credentials, 'token'):
                                _storage_options["GOOGLE_SERVICE_ACCOUNT_TOKEN"] = credentials.token
                        except Exception:
                            pass

                if project_id:
                    _storage_options["GOOGLE_CLOUD_PROJECT"] = project_id

                # Alternative project ID lookup
                if not project_id:
                    project_id = GCPUtilities._get_gcp_project_id()
                    if project_id:
                        _storage_options["GOOGLE_CLOUD_PROJECT"] = project_id

            except Exception as e:
                logger.warning(f"Failed to get GCP credentials: {e}")

        elif self.is_azure:
            # Azure Blob Storage / ADLS configuration
            try:
                credentials = AzureUtilities._get_azure_credentials()
                account_info = AzureUtilities._get_azure_account_info()
                region = AzureUtilities._get_azure_region()

                # Set account information
                if account_info.get("account_name"):
                    _storage_options["AZURE_STORAGE_ACCOUNT_NAME"] = account_info["account_name"]

                # Prefer SAS token, then account key, then managed identity
                if account_info.get("sas_token"):
                    _storage_options["AZURE_STORAGE_SAS_TOKEN"] = account_info["sas_token"]
                elif account_info.get("account_key"):
                    _storage_options["AZURE_STORAGE_ACCOUNT_KEY"] = account_info["account_key"]
                elif credentials:
                    # Use managed identity / service principal
                    _storage_options["AZURE_STORAGE_USE_AZURE_CLI"] = "true"

                    if account_info.get("tenant_id"):
                        _storage_options["AZURE_TENANT_ID"] = account_info["tenant_id"]
                    if account_info.get("client_id"):
                        _storage_options["AZURE_CLIENT_ID"] = account_info["client_id"]
                    if account_info.get("client_secret"):
                        _storage_options["AZURE_CLIENT_SECRET"] = account_info["client_secret"]

                if region:
                    _storage_options["AZURE_REGION"] = region

            except Exception as e:
                logger.warning(f"Failed to get Azure credentials: {e}")

        return _storage_options

    @property
    def max_rows_per_file(self) -> int:
        return self.config.max_rows_per_file

    def _get_default_microbatch_size(self) -> int:
        """Calculate default microbatch size based on target block size."""
        from ray.data.context import DataContext

        target_size = DataContext.get_current().target_max_block_size
        # Use a conservative estimate: assume 1KB per row
        return max(1000, target_size // 1024)

    def _split_blocks_into_microbatches(
        self, blocks: List[Block]
    ) -> Iterator[List[Block]]:
        """Split blocks into microbatches for processing."""
        if not self.enable_microbatch_writes:
            yield blocks
            return

        current_batch = []
        current_size = 0

        for block in blocks:
            block_accessor = BlockAccessor.for_block(block)
            block_rows = block_accessor.num_rows()

            if current_size + block_rows > self.microbatch_size and current_batch:
                yield current_batch
                current_batch = [block]
                current_size = block_rows
            else:
                current_batch.append(block)
                current_size += block_rows

        if current_batch:
            yield current_batch

    def optimize_table(
        self, config: Optional[OptimizationConfig] = None
    ) -> Dict[str, Any]:
        """Perform table optimization operations."""
        config = config or self.optimization_config
        return self._optimizer.optimize(config)

    def compact_table(
        self,
        target_size_bytes: Optional[int] = None,
        partition_filters: Optional[List[Tuple[str, str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Compact table files for better performance."""
        config = OptimizationConfig(
            mode=OptimizationMode.COMPACT,
            target_size_bytes=target_size_bytes,
            partition_filters=partition_filters,
        )
        return self._optimizer.optimize(config)

    def z_order_table(
        self,
        columns: List[str],
        target_size_bytes: Optional[int] = None,
        partition_filters: Optional[List[Tuple[str, str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Z-order table for improved data skipping."""
        config = OptimizationConfig(
            mode=OptimizationMode.Z_ORDER,
            z_order_columns=columns,
            target_size_bytes=target_size_bytes,
            partition_filters=partition_filters,
        )
        return self._optimizer.optimize(config)

    def vacuum_table(
        self, retention_hours: Optional[int] = None
    ) -> List[str]:
        """
        Remove old files that are no longer referenced.

        Note: This method always performs actual file deletion (not a dry run).
        For preview mode, use the standalone vacuum_delta_table function.
        """
        config = OptimizationConfig(
            mode=OptimizationMode.VACUUM,
            retention_hours=retention_hours,
        )
        return self._optimizer.optimize(config)

    @staticmethod
    def _get_table_and_table_uri(
        path: str,
        storage_options: Optional[Dict[str, str]] = None,
    ):
        """Parses the `path`.

        Args:

            path: URI of a table or a DeltaTable object.
            storage_options: Options passed to the native delta filesystem.

        Returns:
            DeltaTable object, URI of the table
        """
        from deltalake.table import DeltaTable

        if not isinstance(path, (str, Path, DeltaTable)):
            raise ValueError("path must be a str, Path or DeltaTable")

        if isinstance(path, (str, Path)):
            table = try_get_deltatable(path, storage_options)
            table_uri = str(path)
        else:
            raise TypeError(f"Invalid path arg, must be str: {path}")

        return (table, table_uri)

    @staticmethod
    def _validate_mode(
        configuration: Optional[Mapping[str, Optional[str]]],
        mode: Optional[WriteMode] = WriteMode.APPEND.value,
    ) -> None:
        config_delta_append_only = (
            configuration and configuration.get("delta.appendOnly", "false") == "true"
        )
        if config_delta_append_only and mode != "append":
            raise ValueError(
                "If configuration has delta.appendOnly = 'true', mode must be 'append'."
                f" Mode is currently {mode}"
            )

    def _extract_schema_and_partition_cols(self, blocks: Iterable[Block]):
        # Finds and returns arrow schema and partition columns from the first non-empty block.
        for block in blocks:
            acc = BlockAccessor.for_block(block)
            if acc.num_rows() > 0:
                # Assumes all blocks have the same schema
                table = acc.to_arrow()
                return table.schema, self.partition_cols or []
        return None, self.partition_cols or []

    def _get_schema(self, write_result: DeltaSinkWriteResult):
        schema = None
        if hasattr(write_result, "schema") and write_result.schema is not None:
            schema = write_result.schema

        elif hasattr(write_result, "write_returns"):
            for res in write_result.write_returns:
                if hasattr(res, "schema") and res.schema is not None:
                    schema = res.schema
                    break

        if schema is not None:
            self.schema = schema

        if self.schema is None:
            raise ValueError(
                "Schema could not be determined for Delta table commit. "
                "Either provide it at construction, or ensure it is available from DataSink write."
            )
        return self.schema

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> DeltaSinkWriteResult:
        blocks = list(blocks)
        if all(BlockAccessor.for_block(block).num_rows() == 0 for block in blocks):
            return DeltaSinkWriteResult(actions=[])

        # Extract or check schema
        if self.schema is None:
            schema, pcols = self._extract_schema_and_partition_cols(blocks)
            if schema is None:
                raise ValueError(
                    "No non-empty data blocks found; unable to infer schema."
                )
            self.schema = schema
            self.partition_cols = pcols

        # Get the table and table URI from the path argument
        table, table_uri = self._get_table_and_table_uri(
            self.path, self.storage_options
        )

        # Update the table to get the latest config in case of a concurrent write
        if table:
            table.update_incremental()

        # Check if the table is append-only and if it matches with the write config
        self._validate_mode(configuration=self.config.configuration, mode=self.mode)

        # Handle merge mode
        if self.mode == WriteMode.MERGE.value:
            return self._write_merge(blocks, table, table_uri)

        # Handle standard write modes (append, overwrite, etc.)
        return self._write_standard(blocks, table, table_uri)

    def _write_merge(
        self, blocks: List[Block], table, table_uri: str
    ) -> DeltaSinkWriteResult:
        """Handle merge operations with microbatch processing."""
        all_merge_metrics = {}
        total_processed = 0

        # Process blocks in microbatches for memory efficiency
        for microbatch_blocks in self._split_blocks_into_microbatches(blocks):
            # Convert blocks to Arrow table
            arrow_tables = []
            for block in microbatch_blocks:
                arrow_table = BlockAccessor.for_block(block).to_arrow()
                if len(arrow_table) > 0:
                    arrow_tables.append(arrow_table)

            if not arrow_tables:
                continue

            # Combine all tables in the microbatch
            combined_table = pa.concat_tables(arrow_tables)
            total_processed += len(combined_table)

            # Perform merge operation
            try:
                merge_metrics = self._merger.merge_microbatch(
                    combined_table, self.merge_config
                )

                # Accumulate metrics
                for key, value in merge_metrics.items():
                    if isinstance(value, (int, float)):
                        all_merge_metrics[key] = all_merge_metrics.get(key, 0) + value
                    else:
                        all_merge_metrics[key] = value

            except Exception as e:
                logger.error(f"Merge operation failed for microbatch: {e}")
                raise

        logger.info(f"Processed {total_processed} rows in merge operation")
        return DeltaSinkWriteResult(
            actions=[], schema=self.schema, merge_metrics=all_merge_metrics
        )

    def _normalize_type_name(self, type_str: str) -> str:
        """Extract and normalize type name from complex type strings."""
        type_str = str(type_str).lower()

        # Handle complex type representations like 'arro3.core.datatype<int64>'
        if "<" in type_str and ">" in type_str:
            # Extract the type from inside brackets
            start = type_str.find("<") + 1
            end = type_str.rfind(">")
            if start > 0 and end > start:
                type_str = type_str[start:end]

        # Normalize common type aliases
        type_str = type_str.strip()
        if type_str in ["utf8", "string"]:
            return "string"
        elif type_str in ["int64", "bigint", "long"]:
            return "int64"
        elif type_str in ["float64", "double"]:
            return "float64"
        elif type_str in ["bool", "boolean"]:
            return "bool"

        return type_str

    def _schemas_compatible(
        self, data_schema: pa.Schema, table_schema: pa.Schema
    ) -> bool:
        """Check if two schemas are compatible, handling equivalent types."""
        if len(data_schema) != len(table_schema):
            return False

        for data_field, table_field in zip(data_schema, table_schema):
            if data_field.name != table_field.name:
                return False

            # Normalize type names for comparison
            data_type = self._normalize_type_name(data_field.type)
            table_type = self._normalize_type_name(table_field.type)

            # Check if normalized types match
            if data_type == table_type:
                continue
            else:
                logger.warning(
                    f"Type mismatch: data_type='{data_type}', table_type='{table_type}' (original: '{data_field.type}' vs '{table_field.type}')"
                )
                return False

        return True

    def _write_standard(
        self, blocks: List[Block], table, table_uri: str
    ) -> DeltaSinkWriteResult:
        """Handle standard write operations (append, overwrite, etc.)."""
        # If the table exists, check if the schema matches
        if table:
            table_schema = table.schema().to_arrow()
            if not self._schemas_compatible(self.schema, table_schema) and not (
                self.mode == WriteMode.OVERWRITE.value and self.config.overwrite_schema
            ):
                raise ValueError(
                    f"Schema mismatch between input data and existing Delta table. "
                    f"This error occurs when column names, types, or order differ between your data and the table. "
                    f"Solutions: 1) Use mode='overwrite' with overwrite_schema=True to replace the table schema, "
                    f"2) Transform your data to match the existing schema, or 3) Write to a new table location.\n\n"
                    f"Existing table schema:\n{table_schema}\n\n"
                    f"Input data schema:\n{self.schema}"
                )
            # If the table exists and the mode is either "error" or "ignore", raise an error or ignore
            if self.mode == WriteMode.ERROR.value:
                raise FileExistsError(
                    "DeltaTable already exists (using writing mode=error)"
                )
            elif self.mode == WriteMode.IGNORE.value:
                logger.info(
                    "DeltaTable already exists, skipping write (using writing mode=ignore)"
                )
                return DeltaSinkWriteResult(actions=[])

            # Ensure that the partition scheme matches the existing table
            if self.partition_cols:
                assert self.partition_cols == table.metadata().partition_columns
            else:
                # If the table exists and the partition_cols is not set, set it to the table's partition columns
                self.partition_cols = table.metadata().partition_columns

        # Write raw Parquet files and collect metadata (scalable approach)
        # Delta log registration happens later in on_write_complete as a single transaction
        add_actions = []

        delta_writer = DeltaUtilities(
            delta_uri=table_uri,
            schema=self.schema,
            filesystem=self.filesystem,
            partition_cols=self.partition_cols,
            max_partitions=self.config.max_partitions,
            config=self.config,
        )

        # Process blocks in microbatches if enabled, otherwise process all at once
        for microbatch_blocks in self._split_blocks_into_microbatches(blocks):
            # Write each batch as Parquet files and collect AddActions
            for batch_ref in microbatch_blocks:
                batch_add_actions = delta_writer.write_raw_data(
                    batch_ref=batch_ref,
                    file_options=self.config.file_options,
                    storage_options=self.storage_options,
                )
                if batch_add_actions:
                    add_actions.extend(batch_add_actions)

        return DeltaSinkWriteResult(actions=add_actions, schema=self.schema)

    def on_write_complete(self, write_result):
        from deltalake import DeltaTable

        # Get schema from write result
        schema = self._get_schema(write_result)

        # Handle merge operations (they don't need file registration)
        if self.mode == WriteMode.MERGE.value:
            return self._handle_merge_completion(write_result)

        # Collect all AddActions from distributed results for standard operations
        final_add_actions = []
        if hasattr(write_result, "write_returns"):  # Distributed execution
            for result in write_result.write_returns:
                if hasattr(result, "actions"):
                    final_add_actions.extend(result.actions)
        elif hasattr(write_result, "actions"):  # Single node execution
            final_add_actions.extend(write_result.actions)

        if len(final_add_actions) == 0:
            return write_result

        # Calculate total rows from AddActions
        total_rows = 0
        for action in final_add_actions:
            if hasattr(action, "stats") and action.stats:
                try:
                    import json

                    stats = json.loads(action.stats)
                    total_rows += stats.get("numRecords", 0)
                except:
                    pass  # Skip if stats parsing fails

        # Check if table exists
        existing_table, table_uri = self._get_table_and_table_uri(
            self.path, self.storage_options
        )

        # Convert our AddActions to DeltaAddActions for the transaction API
        delta_add_actions = []

        for action in final_add_actions:
            # Skip actions with missing required fields
            if not hasattr(action, "path") or not action.path:
                continue
            if not hasattr(action, "size") or action.size is None:
                continue

                # Create DeltaAddAction with proper field types and None handling
            # Try different parameter names for size
            try:
                delta_action = DeltaAddAction(
                    path=str(action.path),
                    size=int(action.size),  # Try 'size' instead of 'size_bytes'
                    partition_values=dict(action.partition_values)
                    if action.partition_values
                    else {},
                    modification_time=int(action.modification_time)
                    if action.modification_time
                    else 0,
                    data_change=bool(action.data_change)
                    if hasattr(action, "data_change")
                    else True,
                    stats=str(action.stats) if action.stats else None,
                )
            except TypeError as e:
                # If that fails, try without stats parameter
                delta_action = DeltaAddAction(
                    path=str(action.path),
                    size=int(action.size),
                    partition_values=dict(action.partition_values)
                    if action.partition_values
                    else {},
                    modification_time=int(action.modification_time)
                    if action.modification_time
                    else 0,
                    data_change=bool(action.data_change)
                    if hasattr(action, "data_change")
                    else True,
                )
            delta_add_actions.append(delta_action)

        if len(delta_add_actions) == 0:
            return write_result

        try:
            if existing_table is None:
                # Create new table first
                delta_schema = DeltaSchema.from_arrow(schema)

                new_table = DeltaTable.create(
                    table_uri=self.path,
                    schema=delta_schema,
                    mode="overwrite",
                    partition_by=self.partition_cols,
                    storage_options=self.storage_options,
                    name=self.config.name,
                    description=self.config.description,
                    configuration=self.config.configuration,
                )

                # Now register the files using write transaction
                new_table.create_write_transaction(
                    actions=delta_add_actions,
                    mode=self.mode,
                    schema=delta_schema,
                    partition_by=self.partition_cols,
                    commit_properties=self.config.commit_properties,
                    post_commithook_properties=self.config.post_commithook_properties,
                )

            else:
                # Append to existing table
                delta_schema = DeltaSchema.from_arrow(schema)

                existing_table.create_write_transaction(
                    actions=delta_add_actions,
                    mode="append",  # Always append to existing table
                    schema=delta_schema,
                    partition_by=self.partition_cols,
                    commit_properties=self.config.commit_properties,
                    post_commithook_properties=self.config.post_commithook_properties,
                )

            # Verify the transaction was successful
            final_table = DeltaTable(self.path, storage_options=self.storage_options)
            final_table.update_incremental()

            # Run optimization if enabled and configured
            if self.enable_optimization and self.optimization_config:
                try:
                    logger.info(f"Running post-write optimization: {self.optimization_config.mode}")
                    optimization_result = self._optimizer.optimize(self.optimization_config)
                    logger.info(f"Post-write optimization completed: {optimization_result}")

                    # Add optimization metrics to write result if possible
                    if hasattr(write_result, 'optimization_metrics'):
                        write_result.optimization_metrics = optimization_result
                    elif isinstance(write_result, DeltaSinkWriteResult):
                        write_result.optimization_metrics = optimization_result

                except Exception as e:
                    logger.warning(
                        f"Post-write optimization failed but write operation succeeded. "
                        f"This is not critical and your data has been written successfully. "
                        f"Optimization error: {e}. "
                        f"You can manually run optimization later using the standalone optimization functions."
                        "Example: ray.data._internal.datasource.delta_datasink.vacuum_delta_table()"
                    )

        except Exception as e:
            return write_result

        return write_result

    def _handle_merge_completion(self, write_result):
        """Handle completion of merge operations."""
        # Collect merge metrics from distributed results
        all_merge_metrics = {}

        if hasattr(write_result, "write_returns"):  # Distributed execution
            for result in write_result.write_returns:
                if hasattr(result, "merge_metrics") and result.merge_metrics:
                    for key, value in result.merge_metrics.items():
                        if isinstance(value, (int, float)):
                            all_merge_metrics[key] = (
                                all_merge_metrics.get(key, 0) + value
                            )
                        else:
                            all_merge_metrics[key] = value
        elif hasattr(write_result, "merge_metrics") and write_result.merge_metrics:
            all_merge_metrics = write_result.merge_metrics

        # Log merge completion
        if all_merge_metrics:

            # Perform post-merge optimization if configured
            if (
                self.optimization_config.mode != OptimizationMode.COMPACT
            ):  # Don't auto-optimize if already configured
                try:
                    # Auto-compact after merge operations for better performance
                    compact_config = OptimizationConfig(
                        mode=OptimizationMode.COMPACT,
                        max_concurrent_tasks=2,  # Conservative for auto-optimization
                    )
                    optimize_metrics = self._optimizer.optimize(compact_config)

                    all_merge_metrics["post_merge_optimization"] = optimize_metrics
                except Exception as e:
                    logger.warning(f"Post-merge optimization failed: {e}")
        else:
            logger.info("Merge operation completed successfully")

        # Run optimization if enabled and configured
        if self.enable_optimization and self.optimization_config:
            try:
                logger.info(f"Running post-merge optimization: {self.optimization_config.mode}")
                optimization_result = self._optimizer.optimize(self.optimization_config)
                logger.info(f"Post-merge optimization completed: {optimization_result}")

                # Add optimization metrics to merge metrics
                if all_merge_metrics:
                    all_merge_metrics["post_merge_optimization"] = optimization_result

                # Add optimization metrics to write result if possible
                if hasattr(write_result, 'optimization_metrics'):
                    write_result.optimization_metrics = optimization_result
                elif isinstance(write_result, DeltaSinkWriteResult):
                    write_result.optimization_metrics = optimization_result

            except Exception as e:
                logger.warning(
                    f"Post-merge optimization failed but merge operation succeeded. "
                    f"This is not critical and your data has been merged successfully. "
                    f"Optimization error: {e}. "
                    f"You can manually run optimization later using the standalone optimization functions."
                    "Example: ray.data._internal.datasource.delta_datasink.vacuum_delta_table()"
                )

        return write_result
