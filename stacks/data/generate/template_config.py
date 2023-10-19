"""Template configuration.

Classes specifying the configuration used for generation of data workloads from templates.
"""
from datetime import date, datetime
from enum import Enum
from typing import Optional, Literal
from pydantic import BaseModel, Field
import yaml
from abc import ABC, abstractmethod

from stacks.data.utils import get_latest_package_version


class DataSourceType(Enum):
    """Enum containing supported data source types."""

    AZURE_SQL = "azure_sql"


class TriggerFrequency(Enum):
    """Enum containing supported trigger frequencies."""

    MINUTE = "Minute"
    HOUR = "Hour"
    DAY = "Day"
    WEEK = "Week"
    MONTH = "Month"


class WorkloadConfigBaseModel(BaseModel, ABC):
    """Based model for shared config.

    Child classes must have workload_type and template_source_folder however abstraction is available with Pydantic
    """

    @property
    @abstractmethod
    def name(path):
        """Forces child classes to have a name, such as the dataset or the pipeline."""
        raise NotImplementedError

    class Config:
        """Configuration for Pydantic model."""

        use_enum_values = True

    ado_variable_groups_nonprod: list[str] = Field(
        description="List of required variable groups in non-production environment."
    )
    ado_variable_groups_prod: list[str] = Field(
        description="List of required variable groups in production environment."
    )
    pipeline_description: str = Field(
        description="Description of the pipeline to be created. Will be used for the Data Factory pipeline description."
    )
    default_arm_deployment_mode: Optional[str] = Field(
        default="Incremental", description="Deployment mode for terraform."
    )
    stacks_data_package_version: str = Field(
        default=get_latest_package_version("stacks-data"),
        description="Version of the stacks-data Python package to use on the job cluster, e.g. 0.3.0.",
        regex=r"\d+\.\d+\.\d+.*",
    )

    @classmethod
    def from_yaml(cls, config_path):
        """Validates a YAML config and returns the Pydantic class representing the data.

        Args:
            config_path: Path to the YAML file containing config
        Returns:
            WorkloadConfigBaseModel of the validated config
        """
        with open(config_path, "r") as file:
            config_dict = yaml.safe_load(file)
        return cls(**config_dict)


class IngestWorkloadConfigModel(WorkloadConfigBaseModel):
    """Pydantic definitions for data ingest workload generation config."""

    workload_type: Literal["Ingest"] = "Ingest"
    template_source_folder: Literal["ingest_template"] = "ingest_template"

    bronze_container: str = Field(default="raw", description="Name of container for Bronze data.")

    dataset_name: str = Field(
        description="Dataset name, used to derive pipeline and linked service names, e.g. AzureSql_Example."
    )
    data_source_type: DataSourceType = Field(description="Data source type.")

    key_vault_linked_service_name: str = Field(
        default="ls_KeyVault", description="Name of the Key Vault linked service in Data Factory."
    )
    data_source_password_key_vault_secret_name: str = Field(
        description="Secret name of the data source password in Key Vault."
    )
    data_source_connection_string_variable_name: str = Field(description="Variable name for the connection string.")
    window_start_default: Optional[date] = Field(
        default="2010-01-01", description="Default window start date in the Data Factory pipeline."
    )
    window_end_default: Optional[date] = Field(
        default="2010-01-31", description="Default window end date in the Data Factory pipeline."
    )
    trigger_start: Optional[datetime] = Field(
        default="2010-01-01T00:00:00Z", description="Start datetime for Data Factory pipeline trigger."
    )
    trigger_end: Optional[datetime] = Field(
        default="2011-12-31T23:59:59Z", description="End datetime for Data Factory pipeline trigger."
    )
    trigger_frequency: Optional[TriggerFrequency] = Field(
        default="Month", description="Frequency for the Data Factory pipeline trigger."
    )
    trigger_interval: Optional[int] = Field(
        default=1, description="Interval value for the Data Factory pipeline trigger."
    )
    trigger_delay: Optional[str] = Field(
        default="02:00:00", description="Delay between Data Factory pipeline triggers, formatted HH:mm:ss"
    )

    @property
    def name(self) -> str:
        """Returns the dataset name."""
        return "ingest_" + self.dataset_name


class ProcessingWorkloadConfigModel(WorkloadConfigBaseModel):
    """Pydantic definitions for data processing workload generation config."""

    workload_type: Literal["processing"] = "processing"
    template_source_folder: Literal["processing_template"] = "processing_template"

    pipeline_name: str = Field(description="Name of the pipeline")

    @property
    def name(self) -> str:
        """Returns the pipeline name."""
        return self.pipeline_name
