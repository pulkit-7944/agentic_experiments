# src/nuvyn_bldr/schemas/sttm.py

from pydantic import BaseModel, Field
from typing import List, Dict, Literal, Optional

class ColumnMapping(BaseModel):
    """Details the mapping for a single column from source to target."""
    source_column: Optional[str] = Field(
        default=None, 
        description="Name of the column in the source file. Can be null for generated columns (e.g., surrogate keys)."
    )
    target_column: str = Field(..., description="Desired name of the column in the target table.")
    target_type: str = Field(..., description="The suggested PySpark data type (e.g., 'StringType()', 'IntegerType()', 'TimestampType()').")
    transformation_rule: str = Field(
        "direct_map", 
        description="A description of the transformation logic (e.g., 'direct_map', 'cast_to_int', 'rename')."
    )

class TableMapping(BaseModel):
    """Defines the mapping for a single target table (fact or dimension)."""
    source_name: str = Field(..., description="The name of the source file this table is derived from.")
    target_table_name: str = Field(..., description="The name of the target table in the data warehouse.")
    table_type: Literal['fact', 'dimension'] = Field(..., description="The type of table in the star schema.")
    primary_key: List[str] = Field(default_factory=list, description="List of columns that form the primary key for this table.")
    foreign_keys: Dict[str, str] = Field(
        default_factory=dict, 
        description="Mapping of foreign key columns in this table to the target dimension table they reference (e.g., {'customer_key': 'dim_customer'})."
    )
    columns: List[ColumnMapping] = Field(..., description="A list of column mappings for this table.")

class STTM(BaseModel):
    """
    Source-to-Target Mapping (STTM) Schema.

    This is the master document produced by the Analyst Agent. It serves as the
    blueprint for the Developer Agent to generate the ETL pipeline.
    """
    sttm_id: str = Field("sttm_v1", description="A version or unique identifier for this mapping document.")
    description: str = Field(
        "A star schema model inferred from the source data profiles.",
        description="A high-level description of the proposed data warehouse model."
    )
    tables: List[TableMapping] = Field(..., description="A list of all the fact and dimension tables to be created.")
