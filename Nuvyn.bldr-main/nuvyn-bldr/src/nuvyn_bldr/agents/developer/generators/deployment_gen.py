# src/nuvyn_bldr/agents/developer/generators/deployment_gen.py

"""
Deployment configuration generator.
"""

import json
import logging
from typing import Dict, Any, List
from jinja2 import Environment, FileSystemLoader, Template
from langchain_core.messages import SystemMessage, HumanMessage

from .base_generator import BaseGenerator
from nuvyn_bldr.core.config import settings

logger = logging.getLogger(__name__)

class DeploymentGenerator(BaseGenerator):
    """Generates deployment configuration using Azure OpenAI."""
    
    def __init__(self, llm_service):
        """Initialize deployment generator with LLM service."""
        self.llm_service = llm_service
        self.template_env = Environment(loader=FileSystemLoader(settings.BASE_DIR / "prompts"))
        
    def generate(self, data: Dict[str, Any]) -> str:
        """Generate deployment configuration from STTM data."""
        try:
            logger.info("Generating deployment configuration...")
            
            # Generate Databricks job configuration
            job_config = self._generate_job_configuration(data)
            
            # Generate cluster configuration
            cluster_config = self._generate_cluster_configuration(data)
            
            # Generate monitoring configuration
            monitoring_config = self._generate_monitoring_configuration(data)
            
            # Combine all configurations
            complete_config = self._combine_configurations(job_config, cluster_config, monitoring_config, data)
            
            logger.info("Deployment configuration generation completed successfully.")
            return complete_config
            
        except Exception as e:
            logger.error(f"Failed to generate deployment configuration: {e}")
            return self._generate_fallback_config(data)
    
    def _generate_job_configuration(self, sttm_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate Databricks job configuration."""
        try:
            # Use LLM to generate job configuration
            prompt = self._build_job_config_prompt(sttm_data)
            
            model = self.llm_service.get_model(
                model_alias="default",
                temperature=0.1,
                max_tokens=2000
            )
            
            response = model.invoke([
                SystemMessage(content="You are an expert Databricks DevOps engineer. Generate only valid JSON configuration."),
                HumanMessage(content=prompt)
            ])
            
            # Extract JSON from response
            config_json = self._extract_json_from_response(response.content)
            return json.loads(config_json) if isinstance(config_json, str) else config_json
            
        except Exception as e:
            logger.error(f"Failed to generate job configuration: {e}")
            return self._generate_basic_job_config(sttm_data)
    
    def _build_job_config_prompt(self, sttm_data: Dict[str, Any]) -> str:
        """Build prompt for job configuration generation."""
        project_name = sttm_data.get("metadata", {}).get("project_name", "Unknown Project")
        business_domain = sttm_data.get("metadata", {}).get("business_domain", "Unknown Domain")
        tables = sttm_data.get("tables", [])
        
        # Calculate estimated data volume
        total_estimated_rows = sum(
            table.get("metadata", {}).get("estimated_row_count", 1000) 
            for table in tables
        )
        
        prompt = f"""
Generate a Databricks job configuration JSON for the ELT pipeline.

Project Details:
- Project Name: {project_name}
- Business Domain: {business_domain}
- Total Tables: {len(tables)}
- Estimated Total Rows: {total_estimated_rows:,}

Tables:
"""
        
        for table in tables:
            table_name = table.get("target_table_name", "unknown")
            table_type = table.get("table_type", "unknown")
            estimated_rows = table.get("metadata", {}).get("estimated_row_count", 1000)
            refresh_freq = table.get("metadata", {}).get("refresh_frequency", "Daily")
            
            prompt += f"""
- {table_name} ({table_type}):
  Estimated Rows: {estimated_rows:,}
  Refresh Frequency: {refresh_freq}
"""
        
        prompt += f"""
Generate a JSON configuration that includes:
1. Job name and description
2. Notebook path configuration
3. Cluster configuration (node type, number of workers)
4. Schedule configuration (based on table refresh frequencies)
5. Parameters for the job
6. Timeout settings
7. Retry configuration
8. Alert settings

Output only valid JSON configuration suitable for Databricks REST API.
"""
        
        return prompt
    
    def _extract_json_from_response(self, llm_output: str) -> str:
        """Extract JSON from LLM response."""
        if "```json" in llm_output:
            return llm_output.split("```json\n")[1].split("```")[0]
        elif "```" in llm_output:
            return llm_output.split("```")[1]
        else:
            import re
            json_match = re.search(r'\{.*\}', llm_output, re.DOTALL)
            if json_match:
                return json_match.group(0)
            return llm_output
    
    def _generate_cluster_configuration(self, sttm_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate cluster configuration based on data volume."""
        # Calculate cluster size based on data volume
        total_estimated_rows = sum(
            table.get("metadata", {}).get("estimated_row_count", 1000) 
            for table in sttm_data.get("tables", [])
        )
        
        # Determine cluster size based on data volume
        if total_estimated_rows < 100000:
            node_type = "Standard_DS3_v2"
            num_workers = 2
        elif total_estimated_rows < 1000000:
            node_type = "Standard_DS4_v2"
            num_workers = 4
        elif total_estimated_rows < 10000000:
            node_type = "Standard_DS5_v2"
            num_workers = 8
        else:
            node_type = "Standard_E8s_v3"
            num_workers = 16
        
        return {
            "node_type_id": node_type,
            "num_workers": num_workers,
            "driver_node_type_id": node_type,
            "spark_version": "13.3.x-scala2.12",
            "spark_conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true"
            },
            "autotermination_minutes": 60,
            "enable_elastic_disk": True
        }
    
    def _generate_monitoring_configuration(self, sttm_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate monitoring and alerting configuration."""
        project_name = sttm_data.get("metadata", {}).get("project_name", "Unknown Project")
        
        return {
            "monitoring": {
                "enabled": True,
                "metrics": [
                    "job_duration",
                    "task_count",
                    "executor_metrics",
                    "memory_usage",
                    "cpu_usage"
                ]
            },
            "alerts": {
                "job_failure": {
                    "enabled": True,
                    "webhook_url": f"https://webhook.example.com/{project_name.lower().replace(' ', '_')}",
                    "email": ["data-team@company.com"]
                },
                "job_timeout": {
                    "enabled": True,
                    "timeout_minutes": 120,
                    "webhook_url": f"https://webhook.example.com/{project_name.lower().replace(' ', '_')}_timeout"
                },
                "data_quality": {
                    "enabled": True,
                    "webhook_url": f"https://webhook.example.com/{project_name.lower().replace(' ', '_')}_dq"
                }
            },
            "logging": {
                "level": "INFO",
                "retention_days": 30,
                "log_path": f"/dbfs/logs/{project_name.lower().replace(' ', '_')}"
            }
        }
    
    def _combine_configurations(self, job_config: Dict[str, Any], cluster_config: Dict[str, Any], 
                              monitoring_config: Dict[str, Any], sttm_data: Dict[str, Any]) -> str:
        """Combine all configurations into a complete deployment config."""
        project_name = sttm_data.get("metadata", {}).get("project_name", "Unknown Project")
        business_domain = sttm_data.get("metadata", {}).get("business_domain", "Unknown Domain")
        
        complete_config = {
            "project_name": project_name,
            "business_domain": business_domain,
            "created_date": "2024-01-15T00:00:00Z",
            "version": "1.0",
            "job_configs": [
                {
                    "job_name": f"{project_name}_ELT_Pipeline",
                    "description": f"ELT pipeline for {project_name} in {business_domain} domain",
                    "notebook_path": f"/Shared/ELT_Pipelines/{project_name.lower().replace(' ', '_')}_pipeline",
                    "cluster_config": cluster_config,
                    "schedule": {
                        "quartz_cron_expression": "0 0 2 * * ?",  # Daily at 2 AM
                        "timezone_id": "UTC"
                    },
                    "parameters": {
                        "project_name": project_name,
                        "environment": "production",
                        "data_quality_enabled": "true"
                    },
                    "timeout_seconds": 7200,  # 2 hours
                    "max_retries": 3,
                    "retry_on_timeout": True
                }
            ],
            "monitoring_config": monitoring_config,
            "metadata": {
                "total_tables": len(sttm_data.get("tables", [])),
                "total_columns": sum(len(table.get("columns", [])) for table in sttm_data.get("tables", [])),
                "estimated_data_volume": sum(
                    table.get("metadata", {}).get("estimated_row_count", 1000) 
                    for table in sttm_data.get("tables", [])
                )
            }
        }
        
        return json.dumps(complete_config, indent=2)
    
    def _generate_fallback_config(self, sttm_data: Dict[str, Any]) -> str:
        """Generate basic fallback configuration if LLM generation fails."""
        project_name = sttm_data.get("metadata", {}).get("project_name", "Unknown Project")
        tables = sttm_data.get("tables", [])
        
        fallback_config = {
            "project_name": project_name,
            "business_domain": sttm_data.get("metadata", {}).get("business_domain", "Unknown Domain"),
            "created_date": "2024-01-15T00:00:00Z",
            "version": "1.0",
            "job_configs": [
                {
                    "job_name": f"{project_name}_ELT_Pipeline",
                    "description": f"ELT pipeline for {project_name}",
                    "notebook_path": f"/Shared/ELT_Pipelines/{project_name.lower().replace(' ', '_')}_pipeline",
                    "cluster_config": {
                        "node_type_id": "Standard_DS3_v2",
                        "num_workers": 2,
                        "driver_node_type_id": "Standard_DS3_v2",
                        "spark_version": "13.3.x-scala2.12",
                        "autotermination_minutes": 60
                    },
                    "schedule": {
                        "quartz_cron_expression": "0 0 2 * * ?",
                        "timezone_id": "UTC"
                    },
                    "parameters": {
                        "project_name": project_name,
                        "environment": "production"
                    },
                    "timeout_seconds": 3600,
                    "max_retries": 3
                }
            ],
            "monitoring_config": {
                "monitoring": {"enabled": True},
                "alerts": {
                    "job_failure": {"enabled": True}
                }
            },
            "metadata": {
                "total_tables": len(tables),
                "total_columns": sum(len(table.get("columns", [])) for table in tables),
                "fallback_generated": True
            }
        }
        
        return json.dumps(fallback_config, indent=2)
    
    def _generate_basic_job_config(self, sttm_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate basic job configuration."""
        project_name = sttm_data.get("metadata", {}).get("project_name", "Unknown Project")
        
        return {
            "job_name": f"{project_name}_ELT_Pipeline",
            "description": f"ELT pipeline for {project_name}",
            "notebook_path": f"/Shared/ELT_Pipelines/{project_name.lower().replace(' ', '_')}_pipeline",
            "cluster_config": {
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 2,
                "driver_node_type_id": "Standard_DS3_v2",
                "spark_version": "13.3.x-scala2.12"
            },
            "schedule": {
                "quartz_cron_expression": "0 0 2 * * ?",
                "timezone_id": "UTC"
            },
            "parameters": {
                "project_name": project_name,
                "environment": "production"
            },
            "timeout_seconds": 3600,
            "max_retries": 3
        } 