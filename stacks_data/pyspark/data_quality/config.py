"""Data Quality configuration.

Classes specifying the configuration used for data quality processing.
"""
from pydantic import BaseModel, Field


class Expectation(BaseModel):
    expectation_type: str = Field(description="The name of the GX expectation class to use.")
    expectation_kwargs: dict = Field(description="The keyword arguments to pass to the expectation class.")


class ValidationConfig(BaseModel):
    column_name: str
    expectations: list[Expectation]


class DatasourceConfig(BaseModel):
    datasource_name: str = Field(description="Data asset name (e.g. table or file name).")
    datasource_type: str = Field(description="Source system type (e.g. table, parquet, csv).")
    data_location: str = Field(
        description="Path to the given data asset within the dq_input_path, or a fully qualified table name."
    )
    expectation_suite_name: str
    validation_config: list[ValidationConfig]


class Config(BaseModel):
    gx_directory_path: str = Field(description="Path to GX Metadata Store.")
    dataset_name: str
    dq_input_path: str = Field(description="Path to where the input data is stored.")
    dq_output_path: str = Field(description="Path to where data quality results will be written.")
    datasource_config: list[DatasourceConfig]
