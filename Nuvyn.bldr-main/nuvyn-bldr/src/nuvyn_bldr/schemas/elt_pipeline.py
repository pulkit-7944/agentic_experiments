# src/nuvyn_bldr/schemas/elt_pipeline.py

from pydantic import BaseModel, Field
from typing import Dict, Any, Optional

class PipelineMetadata(BaseModel):
    """Metadata for the generated ELT pipeline."""
    version: str = Field("1.0", description="Pipeline version")
    created_date: str = Field(..., description="Creation date")
    project_name: str = Field(..., description="Project name")
    total_tables: int = Field(..., description="Total tables processed")
    total_columns: int = Field(..., description="Total columns processed")

class ELTPipeline(BaseModel):
    """Complete ELT pipeline artifacts."""
    pyspark_script: str = Field(..., description="Main PySpark transformation script")
    databricks_notebook: str = Field(..., description="Formatted Databricks notebook")
    data_quality_suite: str = Field(..., description="Data quality validation code")
    deployment_config: Dict[str, Any] = Field(..., description="Deployment configuration")
    metadata: PipelineMetadata = Field(..., description="Pipeline metadata") 