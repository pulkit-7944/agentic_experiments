# src/nuvyn_bldr/agents/developer/agent.py

import json
import logging
import re
from typing import List, Dict, Any
from datetime import datetime

from jinja2 import Environment, FileSystemLoader, Template
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, PrivateAttr

from nuvyn_bldr.core.config import settings
from nuvyn_bldr.core.error_handler import ELTGenerationError, CodeValidationError, DataQualityError, DeploymentError
from nuvyn_bldr.core.llm_service import llm_service
from nuvyn_bldr.schemas.sttm import STTM
from nuvyn_bldr.schemas.elt_pipeline import ELTPipeline, PipelineMetadata
from nuvyn_bldr.schemas.code_validation import ValidationResult
from .generators import PySparkGenerator, DataQualityGenerator, DeploymentGenerator
from .utils import CodeValidator, CodeFormatter

# Configure logging
logger = logging.getLogger(__name__)

class DeveloperAgent(BaseModel):
    """
    The Developer Agent - Generates ELT scripts and Databricks notebooks from STTM.
    
    This agent transforms the Analyst Agent's STTM (Source-to-Target Mapping) output
    into production-ready ELT scripts and Databricks notebooks using Azure OpenAI.
    
    Attributes:
        _llm_service: LLM service for code generation
        _code_generators: Dictionary of specialized code generators
        _prompt_template: Jinja2 template for main ELT generation
        _code_validator: Code validation utility
        _code_formatter: Code formatting utility
    """

    _llm_service: Any = PrivateAttr()
    _code_generators: Dict[str, Any] = PrivateAttr()
    _prompt_template: Template = PrivateAttr()
    _code_validator: CodeValidator = PrivateAttr()
    _code_formatter: CodeFormatter = PrivateAttr()

    def __init__(self, **data):
        """
        Initializes the DeveloperAgent with Azure OpenAI configuration.
        
        Sets up internal tools like code generators, validators, and loads prompt templates.
        """
        super().__init__(**data)
        
        # Initialize LLM service with Azure OpenAI for Developer Agent
        self._llm_service = llm_service
        # Override provider for Developer Agent to use Azure OpenAI
        original_provider = self._llm_service.provider_name
        self._llm_service.provider_name = "azure_openai"
        
        # Initialize code generators
        self._code_generators = {
            "pyspark": PySparkGenerator(self._llm_service),
            "data_quality": DataQualityGenerator(self._llm_service),
            "deployment": DeploymentGenerator(self._llm_service),
        }
        
        # Initialize utilities
        self._code_validator = CodeValidator()
        self._code_formatter = CodeFormatter()
        
        # Set up Jinja2 environment to load the prompt template
        prompt_base_path = settings.BASE_DIR / "prompts"
        env = Environment(
            loader=FileSystemLoader(prompt_base_path),
            trim_blocks=True,
            lstrip_blocks=True
        )
        self._prompt_template = env.get_template("developer/elt_generation_with_path.jinja2")
        
        logger.info("DeveloperAgent initialized successfully with Azure OpenAI.")

    def _build_llm_payload(self, sttm: STTM, source_path: str = "/mnt/data/source_data.csv") -> List[SystemMessage | HumanMessage]:
        """
        Builds the structured message payload for the LLM using the Jinja2 template.

        Args:
            sttm: STTMV1 object containing the data warehouse design.
            source_path: Path to the source data file.

        Returns:
            A list of LangChain message objects to be sent to the LLM.
        """
        # Convert STTM to dictionary for the template
        sttm_dict = sttm.model_dump()
        
        # The Jinja2 template will receive the STTM data and source path
        system_prompt_content = self._prompt_template.render(sttm=sttm_dict, source_path=source_path)
        
        # The system prompt contains all instructions and data
        messages = [
            SystemMessage(content=system_prompt_content),
            HumanMessage(content="Based on the provided STTM document, please generate the complete ELT pipeline code.")
        ]
        return messages

    def _extract_code_from_response(self, llm_output: str) -> str:
        """
        Extracts code from LLM response, handling various formats.

        Args:
            llm_output: Raw LLM response

        Returns:
            Cleaned code string
        """
        # Handle code blocks
        if "```python" in llm_output:
            llm_output = llm_output.split("```python\n")[1].split("```")[0]
        elif "```" in llm_output:
            # Extract code from code blocks without language specification
            llm_output = llm_output.split("```")[1]
        else:
            # Try to find Python code in the response
            import re
            code_match = re.search(r'(from pyspark.*?)(?=\n\n|\Z)', llm_output, re.DOTALL)
            if code_match:
                llm_output = code_match.group(0)
        
        return llm_output.strip()

    def _validate_generated_code(self, code: str, sttm: STTM, source_path: str = None) -> ValidationResult:
        """
        Validates generated code for syntax and logic.

        Args:
            code: Generated PySpark code
            sttm: Original STTM for validation
            source_path: Expected source path for validation

        Returns:
            ValidationResult with validation status and issues
        """
        try:
            # Validate syntax
            syntax_valid = self._code_validator.validate_syntax(code)
            
            # Validate logic against STTM
            logic_valid = self._code_validator.validate_logic(code, sttm.model_dump())
            
            # Validate source path usage
            source_path_valid = True
            source_path_issues = []
            if source_path:
                # Check for placeholder paths
                placeholder_patterns = [
                    r'/path/to/.*\.csv',
                    r'path/to/.*\.csv',
                    r'placeholder.*\.csv',
                    r'example.*\.csv'
                ]
                for pattern in placeholder_patterns:
                    if re.search(pattern, code, re.IGNORECASE):
                        source_path_valid = False
                        source_path_issues.append(f"Found placeholder path pattern: {pattern}")
                
                # Check if actual source path is used
                if source_path not in code:
                    source_path_valid = False
                    source_path_issues.append(f"Expected source path '{source_path}' not found in generated code")
            
            # Calculate overall score
            all_valid = syntax_valid and logic_valid and source_path_valid
            score = 100.0 if all_valid else 50.0
            
            issues = []
            if not syntax_valid:
                issues.append("Syntax validation failed")
            if not logic_valid:
                issues.append("Logic validation failed")
            if not source_path_valid:
                issues.extend(source_path_issues)
            
            return ValidationResult(
                is_valid=all_valid,
                issues=issues,
                score=score,
                metadata={
                    "syntax_valid": syntax_valid, 
                    "logic_valid": logic_valid,
                    "source_path_valid": source_path_valid
                }
            )
            
        except Exception as e:
            logger.error(f"Code validation failed: {e}")
            return ValidationResult(
                is_valid=False,
                issues=[f"Validation error: {str(e)}"],
                score=0.0,
                metadata={"error": str(e)}
            )

    def generate_elt_pipeline(self, sttm: STTM, source_path: str = "/mnt/data/source_data.csv") -> ELTPipeline:
        """
        The main orchestration method for the Developer Agent.

        It takes an STTM document, generates ELT pipeline code using Azure OpenAI,
        validates the generated code, and returns a complete ELT pipeline.

        Args:
            sttm: STTMV1 object containing the data warehouse design.

        Returns:
            A complete ELTPipeline object with all generated artifacts.

        Raises:
            ELTGenerationError: If the LLM fails to produce valid ELT code.
            CodeValidationError: If the generated code fails validation.
        """
        logger.info("ELT pipeline generation process started.")
        
        try:
            # 1. Build the prompt payload for the LLM
            llm_payload = self._build_llm_payload(sttm, source_path)
            
            # 2. Invoke Azure OpenAI for code generation
            logger.info("Invoking Azure OpenAI to generate ELT pipeline...")
            model = self._llm_service.get_model(
                model_alias="default", 
                temperature=0.1,  # Low temperature for consistent code generation
                max_tokens=4000   # Larger context for complex pipelines
            )
            response = model.invoke(llm_payload)
            llm_output = response.content
            
            # 3. Extract and clean the generated code
            pyspark_script = self._extract_code_from_response(llm_output)
            
            # 4. Validate the generated code
            logger.info("Validating the generated ELT code...")
            validation_result = self._validate_generated_code(pyspark_script, sttm, source_path)
            
            if not validation_result.is_valid:
                logger.warning(f"Code validation issues: {validation_result.issues}")
                # For now, we'll continue but log the issues
                # In production, you might want to retry or fail here
            
            # 5. Generate additional components
            logger.info("Generating additional pipeline components...")
            
            # Generate data quality suite
            dq_suite = self._code_generators["data_quality"].generate(sttm.model_dump())
            
            # Generate deployment configuration
            deployment_config = self._code_generators["deployment"].generate(sttm.model_dump())
            
            # Generate Databricks notebook
            notebook_code = self._generate_databricks_notebook(sttm, pyspark_script, source_path)
            
            # 6. Format the generated code
            pyspark_script = self._code_formatter.format_pyspark_code(pyspark_script)
            notebook_code = self._code_formatter.format_notebook_code(notebook_code)
            
            # 7. Create pipeline metadata
            metadata = PipelineMetadata(
                version="1.0",
                created_date=datetime.now().isoformat(),
                project_name=getattr(sttm, 'metadata', {}).get('project_name', "Unknown Project"),
                total_tables=len(sttm.tables),
                total_columns=sum(len(table.columns) for table in sttm.tables)
            )
            
            # 8. Create and return the complete pipeline
            pipeline = ELTPipeline(
                pyspark_script=pyspark_script,
                databricks_notebook=notebook_code,
                data_quality_suite=dq_suite,
                deployment_config=json.loads(deployment_config) if isinstance(deployment_config, str) else deployment_config,
                metadata=metadata
            )
            
            logger.info("ELT pipeline generation completed successfully.")
            return pipeline
            
        except Exception as e:
            logger.error(f"Failed to generate ELT pipeline: {e}")
            raise ELTGenerationError(f"ELT pipeline generation failed: {str(e)}", llm_response=llm_output if 'llm_output' in locals() else None)

    def _generate_databricks_notebook(self, sttm: STTM, pyspark_script: str, source_path: str = "/mnt/data/source_data.csv") -> str:
        """
        Generates a structured Databricks notebook from PySpark script.

        Args:
            sttm: STTMV1 object
            pyspark_script: Generated PySpark script

        Returns:
            Formatted Databricks notebook code
        """
        try:
            # Load notebook template
            prompt_base_path = settings.BASE_DIR / "prompts"
            env = Environment(loader=FileSystemLoader(prompt_base_path))
            notebook_template = env.get_template("developer/notebook_structure.jinja2")
            
            # Generate notebook structure
            notebook_content = notebook_template.render(
                sttm=sttm.model_dump(),
                pyspark_script=pyspark_script,
                source_path=source_path
            )
            
            return notebook_content
            
        except Exception as e:
            logger.error(f"Failed to generate Databricks notebook: {e}")
            # Return a basic notebook structure as fallback
            return f"""# Databricks notebook source
# COMMAND ----------
        # Generated ELT Pipeline for {getattr(sttm, 'metadata', {}).get('project_name', 'Unknown Project')}
# COMMAND ----------

{pyspark_script}
"""

    def generate_elt_pipeline_from_file(self, sttm_file_path: str, source_path: str = "/mnt/data/source_data.csv") -> ELTPipeline:
        """
        Convenience method to generate ELT pipeline from STTM JSON file.

        Args:
            sttm_file_path: Path to STTM JSON file

        Returns:
            Complete ELTPipeline object
        """
        try:
            with open(sttm_file_path, 'r') as f:
                sttm_data = json.load(f)
            
            sttm = STTM.model_validate(sttm_data)
            return self.generate_elt_pipeline(sttm, source_path)
            
        except Exception as e:
            logger.error(f"Failed to load STTM from file {sttm_file_path}: {e}")
            raise ELTGenerationError(f"Failed to load STTM from file: {str(e)}") 