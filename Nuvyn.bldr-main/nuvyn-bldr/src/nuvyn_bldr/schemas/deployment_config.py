# src/nuvyn_bldr/schemas/deployment_config.py

from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List

class ClusterConfig(BaseModel):
    """Databricks cluster configuration."""
    node_type_id: str = Field(..., description="Node type for the cluster")
    num_workers: int = Field(..., description="Number of worker nodes")
    spark_version: str = Field(..., description="Spark version")
    driver_node_type_id: Optional[str] = Field(None, description="Driver node type")

class JobConfig(BaseModel):
    """Databricks job configuration."""
    job_name: str = Field(..., description="Job name")
    notebook_path: str = Field(..., description="Path to the notebook")
    cluster_config: ClusterConfig = Field(..., description="Cluster configuration")
    schedule: Optional[Dict[str, Any]] = Field(None, description="Job schedule")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Job parameters")

class DeploymentConfig(BaseModel):
    """Complete deployment configuration."""
    project_name: str = Field(..., description="Project name")
    job_configs: List[JobConfig] = Field(..., description="List of job configurations")
    monitoring_config: Dict[str, Any] = Field(default_factory=dict, description="Monitoring configuration")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Deployment metadata") 