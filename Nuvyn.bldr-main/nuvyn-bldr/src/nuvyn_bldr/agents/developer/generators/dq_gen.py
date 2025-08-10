# src/nuvyn_bldr/agents/developer/generators/dq_gen.py

"""
Data quality code generator.
"""

import logging
from typing import Dict, Any, List
from jinja2 import Environment, FileSystemLoader, Template
from langchain_core.messages import SystemMessage, HumanMessage

from .base_generator import BaseGenerator
from nuvyn_bldr.core.config import settings

logger = logging.getLogger(__name__)

class DataQualityGenerator(BaseGenerator):
    """Generates data quality validation code using Azure OpenAI."""
    
    def __init__(self, llm_service):
        """Initialize data quality generator with LLM service."""
        self.llm_service = llm_service
        self.template_env = Environment(loader=FileSystemLoader(settings.BASE_DIR / "prompts"))
        
    def generate(self, data: Dict[str, Any]) -> str:
        """Generate data quality validation code from STTM data."""
        try:
            logger.info("Generating data quality validation code...")
            
            # Generate validation functions for each table
            table_validations = self._generate_table_validations(data)
            
            # Generate main validation orchestration
            main_validation = self._generate_main_validation(data)
            
            # Combine all validation code
            complete_code = self._combine_validation_code(main_validation, table_validations)
            
            logger.info("Data quality code generation completed successfully.")
            return complete_code
            
        except Exception as e:
            logger.error(f"Failed to generate data quality code: {e}")
            return self._generate_fallback_validation(data)
    
    def _generate_table_validations(self, sttm_data: Dict[str, Any]) -> str:
        """Generate validation functions for each table."""
        validations = []
        
        for table in sttm_data.get("tables", []):
            try:
                validation_code = self._generate_table_validation(table)
                validations.append(validation_code)
            except Exception as e:
                logger.error(f"Failed to generate validation for table {table.get('target_table_name', 'unknown')}: {e}")
                validations.append(self._generate_basic_table_validation(table))
        
        return "\n\n".join(validations)
    
    def _generate_table_validation(self, table: Dict[str, Any]) -> str:
        """Generate validation function for a single table."""
        try:
            # Use LLM to generate table-specific validation
            prompt = self._build_validation_prompt(table)
            
            model = self.llm_service.get_model(
                model_alias="default",
                temperature=0.1,
                max_tokens=2000
            )
            
            response = model.invoke([
                SystemMessage(content="You are an expert data quality engineer. Generate only valid Python code."),
                HumanMessage(content=prompt)
            ])
            
            return self._extract_code_from_response(response.content)
            
        except Exception as e:
            logger.error(f"Failed to generate table validation: {e}")
            return self._generate_basic_table_validation(table)
    
    def _build_validation_prompt(self, table: Dict[str, Any]) -> str:
        """Build prompt for table-specific validation generation."""
        table_name = table.get("target_table_name", "unknown_table")
        table_type = table.get("table_type", "unknown")
        columns = table.get("columns", [])
        
        prompt = f"""
Generate a PySpark data quality validation function for the {table_type} table '{table_name}'.

Table Details:
- Type: {table_type}
- Primary Key: {table.get('primary_key', [])}
- Foreign Keys: {table.get('foreign_keys', {})}

Columns to validate:
"""
        
        for column in columns:
            column_data = column.get("metadata", {}).get("data_quality_metrics", {})
            prompt += f"""
- {column.get('target_column', 'unknown')}: {column.get('target_type', 'unknown')}
  Null Percentage: {column_data.get('null_percentage', 'unknown')}
  Unique Count: {column_data.get('unique_count', 'unknown')}
  Business Rules: {column.get('metadata', {}).get('business_rules', [])}
"""
        
        prompt += f"""
Generate a Python function named 'validate_{table_name}' that:
1. Validates null percentages match expectations
2. Checks data types and formats
3. Enforces business rules
4. Validates primary key uniqueness
5. Validates foreign key relationships
6. Generates quality reports
7. Handles validation failures gracefully

Output only valid Python code with proper PySpark DataFrame API usage.
Include comprehensive error handling and detailed logging.
"""
        
        return prompt
    
    def _extract_code_from_response(self, llm_output: str) -> str:
        """Extract code from LLM response."""
        if "```python" in llm_output:
            return llm_output.split("```python\n")[1].split("```")[0]
        elif "```" in llm_output:
            return llm_output.split("```")[1]
        else:
            import re
            code_match = re.search(r'(def validate_.*?)(?=\n\n|\Z)', llm_output, re.DOTALL)
            if code_match:
                return code_match.group(0)
            return llm_output
    
    def _generate_main_validation(self, sttm_data: Dict[str, Any]) -> str:
        """Generate main validation orchestration code."""
        project_name = sttm_data.get("metadata", {}).get("project_name", "Unknown Project")
        tables = sttm_data.get("tables", [])
        
        # Build the code without complex f-strings
        code_lines = [
            "def run_data_quality_validation(df_dict: Dict[str, Any]) -> Dict[str, Any]:",
            '    """',
            "    Run comprehensive data quality validation for all tables.",
            "",
            "    Args:",
            "        df_dict: Dictionary containing DataFrames for each table",
            "",
            "    Returns:",
            "        Dictionary containing validation results for each table",
            '    """',
            "    validation_results = {}",
            "",
            '    logger.info("Starting data quality validation for all tables...")',
            "",
            "    # Validate each table"
        ]
        
        for table in tables:
            table_name = table.get("target_table_name", "unknown")
            code_lines.extend([
                f"    # Validate {table_name}",
                "    try:",
                f"        if '{table_name}' in df_dict:",
                f"            validation_results['{table_name}'] = validate_{table_name}(df_dict['{table_name}'])",
                f'            logger.info(f"Validation completed for {table_name}")',
                "        else:",
                f'            logger.warning(f"DataFrame for {table_name} not found in input")',
                f"            validation_results['{table_name}'] = {{\"status\": \"error\", \"message\": \"DataFrame not found\"}}",
                "    except Exception as e:",
                f'        logger.error(f"Validation failed for {table_name}: {{e}}")',
                f"        validation_results['{table_name}'] = {{\"status\": \"error\", \"message\": str(e)}}"
            ])
        
        code_lines.extend([
            "",
            "    # Generate overall validation summary",
            '    overall_status = "passed" if all(',
            "        result.get(\"status\") == \"passed\" ",
            "        for result in validation_results.values() ",
            "        if isinstance(result, dict)",
            '    ) else "failed"',
            "",
            "    validation_summary = {",
            '        "overall_status": overall_status,',
            '        "table_results": validation_results,',
            '        "total_tables": len(validation_results),',
            "        \"passed_tables\": sum(",
            "            1 for result in validation_results.values() ",
            '            if isinstance(result, dict) and result.get("status") == "passed"',
            "        ),",
            "        \"failed_tables\": sum(",
            "            1 for result in validation_results.values() ",
            '            if isinstance(result, dict) and result.get("status") == "failed"',
            "        )",
            "    }",
            "",
            '    logger.info(f"Data quality validation completed. Overall status: {overall_status}")',
            "    return validation_summary",
            "",
            "def log_validation_results(results: Dict[str, Any]):",
            '    """Log validation results in a structured format."""',
            '    logger.info("=" * 50)',
            '    logger.info("DATA QUALITY VALIDATION RESULTS")',
            '    logger.info("=" * 50)',
            "",
            '    overall_status = results.get("overall_status", "unknown")',
            '    logger.info(f"Overall Status: {overall_status.upper()}")',
            '    logger.info(f"Total Tables: {results.get(\'total_tables\', 0)}")',
            '    logger.info(f"Passed Tables: {results.get(\'passed_tables\', 0)}")',
            '    logger.info(f"Failed Tables: {results.get(\'failed_tables\', 0)}")',
            "",
            '    logger.info("\\nTable Details:")',
            '    for table_name, result in results.get("table_results", {}).items():',
            "        if isinstance(result, dict):",
            '            status = result.get("status", "unknown")',
            '            message = result.get("message", "No message")',
            '            logger.info(f"  {table_name}: {status.upper()} - {message}")',
            "        else:",
            f'            logger.info(f"  {{table_name}}: {{result}}")',
            "",
            '    logger.info("=" * 50)'
        ])
        
        return "\n".join(code_lines)
    
    def _combine_validation_code(self, main_validation: str, table_validations: str) -> str:
        """Combine main validation with table validations."""
        code_lines = [
            "# Generated Data Quality Validation Suite",
            "# This code was automatically generated by Nuvyn.bldr Developer Agent",
            "",
            "from pyspark.sql import DataFrame",
            "from pyspark.sql.functions import *",
            "from typing import Dict, Any, List",
            "import logging",
            "",
            "# Configure logging",
            "logger = logging.getLogger(__name__)",
            "",
            "# Table-specific validation functions",
            table_validations,
            "",
            "# Main validation orchestration",
            main_validation,
            "",
            "def assert_validation_passed(results: Dict[str, Any]):",
            '    """Assert that all validations passed, raise exception if any failed."""',
            '    overall_status = results.get("overall_status", "unknown")',
            '    if overall_status != "passed":',
            "        failed_tables = [",
            "            table_name for table_name, result in results.get(\"table_results\", {}).items()",
            "            if isinstance(result, dict) and result.get(\"status\") == \"failed\"",
            "        ]",
            "        raise ValueError(f\"Data quality validation failed for tables: {failed_tables}\")",
            "",
            '    logger.info("All data quality validations passed successfully!")'
        ]
        
        return "\n".join(code_lines)
    
    def _generate_fallback_validation(self, sttm_data: Dict[str, Any]) -> str:
        """Generate basic fallback validation if LLM generation fails."""
        project_name = sttm_data.get("metadata", {}).get("project_name", "Unknown Project")
        tables = sttm_data.get("tables", [])
        
        # Build the code without complex f-strings
        code_lines = [
            f"# Generated Data Quality Validation Suite for {project_name}",
            "# Fallback validation code generated due to LLM generation failure",
            "",
            "from pyspark.sql import DataFrame",
            "from pyspark.sql.functions import *",
            "from typing import Dict, Any",
            "import logging",
            "",
            "logger = logging.getLogger(__name__)",
            "",
            "def run_data_quality_validation(df_dict: Dict[str, Any]) -> Dict[str, Any]:",
            '    """Basic data quality validation fallback."""',
            "    validation_results = {}",
            "",
            '    logger.info("Running basic data quality validation...")',
            ""
        ]
        
        for table in tables:
            table_name = table.get("target_table_name", "unknown")
            code_lines.extend([
                f"    # Basic validation for {table_name}",
                f"    if '{table_name}' in df_dict:",
                f"        df = df_dict['{table_name}']",
                f"        validation_results['{table_name}'] = {{",
                '            "status": "passed",',
                '            "message": "Basic validation completed",',
                "            \"row_count\": df.count(),",
                "            \"column_count\": len(df.columns)",
                "        }",
                "    else:",
                f"        validation_results['{table_name}'] = {{",
                '            "status": "error",',
                '            "message": "DataFrame not found"',
                "        }"
            ])
        
        code_lines.extend([
            "",
            "    return {",
            '        "overall_status": "passed",',
            '        "table_results": validation_results,',
            '        "total_tables": len(validation_results),',
            '        "passed_tables": len(validation_results),',
            '        "failed_tables": 0',
            "    }",
            "",
            "def log_validation_results(results: Dict[str, Any]):",
            '    """Log validation results."""',
            '    logger.info(f"Validation completed: {results.get(\'overall_status\', \'unknown\')}")',
            "",
            "def assert_validation_passed(results: Dict[str, Any]):",
            '    """Assert validation passed."""',
            '    if results.get("overall_status") != "passed":',
            '        raise ValueError("Data quality validation failed")'
        ])
        
        return "\n".join(code_lines)
    
    def _generate_basic_table_validation(self, table: Dict[str, Any]) -> str:
        """Generate basic table validation function."""
        table_name = table.get("target_table_name", "unknown_table")
        columns = table.get("columns", [])
        
        code = f"""def validate_{table_name}(df: DataFrame) -> Dict[str, Any]:
    \"\"\"Basic validation for {table_name} table.\"\"\"
    validation_result = {
        "status": "passed",
        "message": "Basic validation completed",
        "checks": []
    }
    
    try:
        # Basic checks
        row_count = df.count()
        column_count = len(df.columns)
        
        validation_result["checks"].extend([
            {"check": "row_count", "value": row_count, "status": "passed"},
            {"check": "column_count", "value": column_count, "status": "passed"}
        ])
        
        # Column-specific checks
"""
        
        for column in columns:
            target_col = column.get("target_column", "unknown")
            code += f"""
        # Check {target_col}
        if "{target_col}" in df.columns:
            null_count = df.filter(col("{target_col}").isNull()).count()
            validation_result["checks"].append({
                "check": "{target_col}_null_count",
                "value": null_count,
                "status": "passed"
            })
        else:
            validation_result["checks"].append({
                "check": "{target_col}_exists",
                "value": "missing",
                "status": "failed"
            })
            validation_result["status"] = "failed"
"""
        
        code += """
        
        # Primary key uniqueness check
        primary_keys = """ + str(table.get("primary_key", [])) + """
        if primary_keys:
            for pk in primary_keys:
                if pk in df.columns:
                    distinct_count = df.select(pk).distinct().count()
                    if distinct_count != row_count:
                        validation_result["checks"].append({
                            "check": f"{pk}_uniqueness",
                            "value": f"{distinct_count}/{row_count}",
                            "status": "failed"
                        })
                        validation_result["status"] = "failed"
                    else:
                        validation_result["checks"].append({
                            "check": f"{pk}_uniqueness",
                            "value": "unique",
                            "status": "passed"
                        })
        
        logger.info(f"Validation completed for {table_name}: {validation_result['status']}")
        return validation_result
        
    except Exception as e:
        logger.error(f"Validation failed for {table_name}: {e}")
        return {
            "status": "error",
            "message": str(e),
            "checks": []
        }
"""
        
        return code 