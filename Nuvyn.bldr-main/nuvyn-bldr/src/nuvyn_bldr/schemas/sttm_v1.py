# src/nuvyn_bldr/schemas/sttm_v1.py

from pydantic import BaseModel, Field
from typing import List, Dict, Literal, Optional, Union

class DataQualityMetric(BaseModel):
    """Data Quality Metrics for a column."""
    null_percentage: Optional[float] = Field(None, description="Percentage of null values")
    unique_count: Optional[int] = Field(None, description="Number of unique values")
    cardinality: Optional[str] = Field(None, description="Cardinality: low, medium, high")

class ColumnMetadata(BaseModel):
    """Essential metadata for a column."""
    business_description: Optional[str] = Field(None, description="Business description")
    source_data_type: Optional[str] = Field(None, description="Original data type")
    target_data_type: str = Field(..., description="Target data type")
    data_quality_metrics: Optional[DataQualityMetric] = Field(None, description="DQM")
    is_sensitive: Optional[bool] = Field(False, description="Contains sensitive data")
    is_required: Optional[bool] = Field(True, description="Required in target")

class ColumnMapping(BaseModel):
    """Details the mapping for a single column from source to target with metadata."""
    source_column: Optional[str] = Field(
        default=None,
        description="Name of the column in the source file. Can be null for generated columns."
    )
    target_column: str = Field(..., description="Desired name of the column in the target table.")
    target_type: str = Field(..., description="The suggested PySpark data type.")
    transformation_rule: str = Field(
        "direct_map",
        description="A description of the transformation logic."
    )
    metadata: Optional[ColumnMetadata] = Field(None, description="Column metadata")

class TableMetadata(BaseModel):
    """Essential metadata for a table."""
    business_description: Optional[str] = Field(None, description="Business description")
    grain: Optional[str] = Field(None, description="The grain/level of detail")
    source_system: Optional[str] = Field(None, description="Source system")
    estimated_row_count: Optional[int] = Field(None, description="Estimated rows")

class TableMapping(BaseModel):
    """Defines the mapping for a single target table with metadata."""
    source_name: Optional[str] = Field(None, description="The name of the source file. Can be null for derived/generated tables.")
    target_table_name: str = Field(..., description="The name of the target table.")
    table_type: Literal['fact', 'dimension'] = Field(..., description="The type of table.")
    primary_key: List[str] = Field(default_factory=list, description="Primary key columns.")
    foreign_keys: Dict[str, str] = Field(
        default_factory=dict,
        description="Foreign key mappings."
    )
    columns: List[ColumnMapping] = Field(..., description="Column mappings.")
    metadata: Optional[TableMetadata] = Field(None, description="Table metadata")

class STTMMetadata(BaseModel):
    """Essential metadata for the entire STTM document."""
    version: str = Field("1.0", description="Version of the STTM")
    created_date: Optional[str] = Field(None, description="Creation date")
    project_name: Optional[str] = Field(None, description="Project name")
    business_domain: Optional[str] = Field(None, description="Business domain")
    total_tables: Optional[int] = Field(None, description="Total tables")
    total_columns: Optional[int] = Field(None, description="Total columns")
    coverage_percentage: Optional[float] = Field(None, description="Column coverage %")

class STTMV1(BaseModel):
    """
    Enhanced Source-to-Target Mapping (STTM) Schema V1 with essential metadata.

    This is the enhanced master document produced by the Analyst Agent. It serves as the
    complete blueprint and documentation for the Developer Agent to generate the ETL pipeline.
    """
    sttm_id: str = Field("sttm_v1", description="A version or unique identifier for this mapping document.")
    description: str = Field(
        "A star schema model inferred from the source data profiles.",
        description="A high-level description of the proposed data warehouse model."
    )
    tables: List[TableMapping] = Field(..., description="A list of all the fact and dimension tables to be created.")
    metadata: Optional[STTMMetadata] = Field(None, description="Essential metadata for the entire STTM document")