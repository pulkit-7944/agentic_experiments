# src/nuvyn_bldr/agents/developer/generators/pyspark_gen.py

"""
PySpark code generator for ELT pipelines.
"""

import logging
from typing import Dict, Any, List
from jinja2 import Environment, FileSystemLoader, Template
from langchain_core.messages import SystemMessage, HumanMessage

from .base_generator import BaseGenerator
from nuvyn_bldr.core.config import settings

logger = logging.getLogger(__name__)

class PySparkGenerator(BaseGenerator):
    """Generates PySpark transformation code using Azure OpenAI."""
    
    def __init__(self, llm_service):
        """Initialize PySpark generator with LLM service."""
        self.llm_service = llm_service
        self.template_env = Environment(loader=FileSystemLoader(settings.BASE_DIR / "prompts"))
        
    def generate(self, data: Dict[str, Any]) -> str:
        """Generate PySpark code from STTM data."""
        try:
            logger.info("Generating PySpark ELT pipeline code...")
            
            # Generate main ELT pipeline
            main_pipeline = self._generate_main_pipeline(data)
            
            # Generate individual table processing functions
            table_functions = self._generate_table_functions(data)
            
            # Combine all code
            complete_code = self._combine_code(main_pipeline, table_functions)
            
            logger.info("PySpark code generation completed successfully.")
            return complete_code
            
        except Exception as e:
            logger.error(f"Failed to generate PySpark code: {e}")
            return self._generate_fallback_code(data)
    
    def _generate_main_pipeline(self, sttm_data: Dict[str, Any]) -> str:
        """Generate the main ELT pipeline orchestration code."""
        try:
            # Use the pyspark_code template for main pipeline
            template = self.template_env.get_template("developer/pyspark_code.jinja2")
            
            # Prepare data for template
            template_data = {
                "sttm": sttm_data,
                "project_name": sttm_data.get("metadata", {}).get("project_name", "Unknown Project"),
                "tables": sttm_data.get("tables", [])
            }
            
            return template.render(**template_data)
            
        except Exception as e:
            logger.error(f"Failed to generate main pipeline: {e}")
            return self._generate_basic_pipeline(sttm_data)
    
    def _generate_table_functions(self, sttm_data: Dict[str, Any]) -> str:
        """Generate individual table processing functions."""
        functions = []
        
        for table in sttm_data.get("tables", []):
            try:
                function_code = self._generate_table_function(table)
                functions.append(function_code)
            except Exception as e:
                logger.error(f"Failed to generate function for table {table.get('target_table_name', 'unknown')}: {e}")
                functions.append(self._generate_basic_table_function(table))
        
        return "\n\n".join(functions)
    
    def _generate_table_function(self, table: Dict[str, Any]) -> str:
        """Generate processing function for a single table."""
        try:
            # Use LLM to generate table-specific code
            prompt = self._build_table_prompt(table)
            
            model = self.llm_service.get_model(
                model_alias="default",
                temperature=0.1,
                max_tokens=2000
            )
            
            response = model.invoke([
                SystemMessage(content="You are an expert PySpark developer. Generate only valid Python code."),
                HumanMessage(content=prompt)
            ])
            
            return self._extract_code_from_response(response.content)
            
        except Exception as e:
            logger.error(f"Failed to generate table function: {e}")
            return self._generate_basic_table_function(table)
    
    def _build_table_prompt(self, table: Dict[str, Any]) -> str:
        """Build prompt for table-specific code generation."""
        table_name = table.get("target_table_name", "unknown_table")
        table_type = table.get("table_type", "unknown")
        source_name = table.get("source_name", "unknown_source")
        columns = table.get("columns", [])
        
        prompt = f"""
Generate a PySpark function to process the {table_type} table '{table_name}' from source '{source_name}'.

Table Details:
- Type: {table_type}
- Source: {source_name}
- Primary Key: {table.get('primary_key', [])}
- Foreign Keys: {table.get('foreign_keys', {})}

Columns to process:
"""
        
        for column in columns:
            prompt += f"""
- {column.get('target_column', 'unknown')}: {column.get('target_type', 'unknown')}
  Source: {column.get('source_column', 'unknown')}
  Transformation: {column.get('transformation_rule', 'direct_map')}
  Description: {column.get('metadata', {}).get('business_description', 'No description')}
"""
        
        prompt += f"""
Generate a Python function named 'process_{table_name}' that:
1. Reads the source data from '{source_name}'
2. Applies all column transformations exactly as specified
3. Handles data types correctly
4. Returns the processed DataFrame

Output only valid Python code with proper PySpark DataFrame API usage.
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
            code_match = re.search(r'(def process_.*?)(?=\n\n|\Z)', llm_output, re.DOTALL)
            if code_match:
                return code_match.group(0)
            return llm_output
    
    def _combine_code(self, main_pipeline: str, table_functions: str) -> str:
        """Combine main pipeline with table functions."""
        return f"""# Generated PySpark ELT Pipeline
# This code was automatically generated by Nuvyn.bldr Developer Agent

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logger = logging.getLogger(__name__)

def create_spark_session(app_name: str = "ELT_Pipeline"):
    \"\"\"Create and configure Spark session.\"\"\"
    return SparkSession.builder \\
        .appName(app_name) \\
        .config("spark.sql.adaptive.enabled", "true") \\
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
        .getOrCreate()

# Table processing functions
{table_functions}

# Main pipeline orchestration
{main_pipeline}

if __name__ == "__main__":
    main()
"""
    
    def _generate_fallback_code(self, sttm_data: Dict[str, Any]) -> str:
        """Generate basic fallback code if LLM generation fails."""
        project_name = sttm_data.get("metadata", {}).get("project_name", "Unknown Project")
        tables = sttm_data.get("tables", [])
        
        code = f"""# Generated PySpark ELT Pipeline for {project_name}
# Fallback code generated due to LLM generation failure

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \\
        .appName("{project_name}_ELT") \\
        .getOrCreate()

def main():
    spark = create_spark_session()
    
    # Process tables
"""
        
        for table in tables:
            table_name = table.get("target_table_name", "unknown")
            source_name = table.get("source_name", "unknown")
            
            code += f"""
    # Process {table_name}
    {table_name}_df = spark.read.csv("{source_name}")
    # TODO: Add column transformations based on STTM
    {table_name}_df.write.mode("overwrite").saveAsTable("{table_name}")
"""
        
        code += """
    print("ELT pipeline completed successfully!")

if __name__ == "__main__":
    main()
"""
        
        return code
    
    def _generate_basic_pipeline(self, sttm_data: Dict[str, Any]) -> str:
        """Generate basic pipeline structure."""
        project_name = sttm_data.get("metadata", {}).get("project_name", "Unknown Project")
        tables = sttm_data.get("tables", [])
        
        code = f"""def main():
    spark = create_spark_session()
    
    # Process dimension tables first
"""
        
        # Add dimension tables
        for table in tables:
            if table.get("table_type") == "dimension":
                table_name = table.get("target_table_name", "unknown")
                code += f"    {table_name}_df = process_{table_name}()\n"
        
        code += "\n    # Process fact tables\n"
        
        # Add fact tables
        for table in tables:
            if table.get("table_type") == "fact":
                table_name = table.get("target_table_name", "unknown")
                code += f"    {table_name}_df = process_{table_name}()\n"
        
        code += "\n    # Write to target tables\n"
        for table in tables:
            table_name = table.get("target_table_name", "unknown")
            code += f"    {table_name}_df.write.mode(\"overwrite\").saveAsTable(\"{table_name}\")\n"
        
        code += '\n    print("ELT pipeline completed successfully!")\n'
        
        return code
    
    def _generate_basic_table_function(self, table: Dict[str, Any]) -> str:
        """Generate basic table processing function."""
        table_name = table.get("target_table_name", "unknown_table")
        source_name = table.get("source_name", "unknown_source")
        columns = table.get("columns", [])
        
        code = f"""def process_{table_name}():
    \"\"\"Process {table_name} table from {source_name}.\"\"\"
    df = spark.read.csv("{source_name}")
    
    # Apply column transformations
"""
        
        for column in columns:
            target_col = column.get("target_column", "unknown")
            source_col = column.get("source_column", "unknown")
            transformation = column.get("transformation_rule", "direct_map")
            
            if transformation == "direct_map":
                code += f"    df = df.withColumn(\"{target_col}\", col(\"{source_col}\"))\n"
            elif transformation == "cast_to_string":
                code += f"    df = df.withColumn(\"{target_col}\", col(\"{source_col}\").cast(\"string\"))\n"
            elif transformation == "cast_to_double":
                code += f"    df = df.withColumn(\"{target_col}\", col(\"{source_col}\").cast(\"double\"))\n"
            elif transformation == "cast_to_int":
                code += f"    df = df.withColumn(\"{target_col}\", col(\"{source_col}\").cast(\"int\"))\n"
            elif transformation == "trim_upper":
                code += f"    df = df.withColumn(\"{target_col}\", upper(trim(col(\"{source_col}\"))))\n"
            else:
                code += f"    df = df.withColumn(\"{target_col}\", col(\"{source_col}\"))  # TODO: Implement {transformation}\n"
        
        code += "\n    return df\n"
        
        return code 