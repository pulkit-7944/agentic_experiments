# src/nuvyn_bldr/agents/analyst/agent.py

import json
import logging
from typing import List

from jinja2 import Environment, FileSystemLoader, Template
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, PrivateAttr
import pandas as pd

from nuvyn_bldr.core.config import settings
from nuvyn_bldr.core.error_handler import STTMGenerationError
from nuvyn_bldr.core.llm_service import llm_service
from nuvyn_bldr.core.profiling import Profiler, SamplerConfig, TableProfile
from nuvyn_bldr.core.data_source_service import DataSourceService, DataSourceError
from nuvyn_bldr.schemas.sttm import STTM


# Configure logging
logger = logging.getLogger(__name__)

class AnalystAgent(BaseModel):
    """
    The Data Architect Assistant agent. (Pydantic-based)

    This agent orchestrates the process of analyzing raw data files,
    using an LLM to infer a star schema, and generating a structured
    Source-to-Target Mapping (STTM) document. By using Pydantic, we can
    easily add configurable fields to the agent in the future.

    Attributes:
        _profiler (Profiler): A private attribute holding the profiling utility.
        _prompt_template (Template): A private attribute holding the Jinja2 prompt.
    """

    _profiler: Profiler = PrivateAttr()
    _prompt_template: Template = PrivateAttr()
    _data_source_service: DataSourceService = PrivateAttr()

    def __init__(self, **data):
        """
        Initializes the AnalystAgent.
        
        Sets up internal tools like the profiler and loads the prompt template.
        """
        super().__init__(**data)
        self._profiler = Profiler()
        
        # Initialize data source service with Azure credentials
        self._data_source_service = DataSourceService(
            azure_connection_string=settings.AZURE_STORAGE_CONNECTION_STRING,
            azure_account_name=settings.AZURE_STORAGE_ACCOUNT_NAME,
            azure_account_key=settings.AZURE_STORAGE_ACCOUNT_KEY,
            azure_sas_token=settings.AZURE_STORAGE_SAS_TOKEN
        )
        
        # Set up Jinja2 environment to load the prompt template
        prompt_base_path = settings.BASE_DIR / "prompts"
        env = Environment(
            loader=FileSystemLoader(prompt_base_path),
            trim_blocks=True,
            lstrip_blocks=True
        )
        # self._prompt_template = env.get_template("analyst/sttm_generation.jinja2")
        # logger.info("AnalystAgent initialized successfully.")

        # self._prompt_template = env.get_template("analyst/advance_sttm_generation.jinja2")
        # logger.info("AnalystAgent initialized successfully.")

        # self._prompt_template = env.get_template("analyst/universal_sttm_generation.jinja2")
        # self._prompt_template = env.get_template("analyst/universal_sttm_generation2.jinja2")
        self._prompt_template = env.get_template("analyst/universal_sttm_generation_with_housekeeping.jinja2")
        logger.info("AnalystAgent initialized successfully with housekeeping prompt.")

    def _run_profiling(self, source_paths: List[str]) -> List[TableProfile]:
        """
        Reads and profiles data from multiple sources (local files, Azure blob, etc.).

        Args:
            source_paths: A list of string paths to the data sources.

        Returns:
            A list of TableProfile objects, one for each source.
        """
        logger.info(f"Starting profiling for {len(source_paths)} source(s).")
        logger.info(f"Supported protocols: {', '.join(self._data_source_service.list_supported_protocols())}")
        
        profiles = []
        for source_path in source_paths:
            try:
                # Validate source path
                if not self._data_source_service.validate_source(source_path):
                    logger.warning(f"Unsupported data source: {source_path}")
                    continue
                
                # Read data using the appropriate data source
                logger.info(f"Reading data from: {source_path}")
                df = self._data_source_service.read_data(source_path)
                
                # Get source name for profiling
                source_name = self._data_source_service.get_source_name(source_path)
                
                # Using the default sampler from our previous discussion as an example
                sampler_config = SamplerConfig(strategy="top_n", value=1000)

                profile = self._profiler.profile_dataframe(
                    df=df,
                    source_name=source_name,
                    sampler_config=sampler_config
                )
                profiles.append(profile)
                logger.info(f"Successfully profiled {source_path} -> {source_name}")
                
            except DataSourceError as e:
                logger.error(f"Data source error for {source_path}: {e}")
                continue
            except Exception as e:
                logger.error(f"Failed to read or profile source: {source_path}. Error: {e}")
                continue
                
        if not profiles:
            logger.error("No data sources could be profiled successfully.")
            
        return profiles

    def _build_llm_payload(self, profiles: List[TableProfile]) -> List[SystemMessage | HumanMessage]:
        """
        Builds the structured message payload for the LLM using the Jinja2 template.

        Args:
            profiles: A list of TableProfile objects.

        Returns:
            A list of LangChain message objects to be sent to the LLM.
        """
        # Convert TableProfile objects to dictionaries for the template
        profile_dicts = [p.model_dump() for p in profiles]
        
        # The Jinja2 template will receive the list of profiles and the schema of the STTM
        system_prompt_content = self._prompt_template.render(
            table_profiles=profile_dicts,
            sttm_schema=STTM.model_json_schema() 
        )
        
        # The system prompt contains all instructions and data
        messages = [
            SystemMessage(content=system_prompt_content),
            HumanMessage(content="Based on the provided data profiles and instructions, please generate the STTM JSON document.")
        ]
        return messages

    def generate_sttm(self, file_paths: List[str]) -> STTM:
        """
        The main orchestration method for the Analyst Agent.

        It takes a list of file paths, profiles them, invokes an LLM to design
        a star schema, and returns a validated STTM.

        Args:
            file_paths: A list of paths to the raw data files.

        Returns:
            A validated STTM Pydantic object.

        Raises:
            STTMGenerationError: If the LLM fails to produce a valid STTM document.
        """
        logger.info("STTM generation process started.")
        
        # 1. Profile all data sources
        table_profiles = self._run_profiling(file_paths)
        if not table_profiles:
            raise STTMGenerationError("No data could be profiled. Aborting STTM generation.")

        # 2. Build the prompt payload for the LLM
        llm_payload = self._build_llm_payload(table_profiles)

        # 3. Invoke the LLM
        logger.info("Invoking LLM to generate STTM...")
        model = llm_service.get_model(model_alias="default", temperature=0.1)
        response = model.invoke(llm_payload)
        llm_output = response.content
        
        # The LLM might return the JSON inside a code block or with extra text, so we clean it.
        if "```json" in llm_output:
            llm_output = llm_output.split("```json\n")[1].split("```")[0]
        elif "```" in llm_output:
            # Extract JSON from code blocks without language specification
            llm_output = llm_output.split("```")[1]
        else:
            # Try to find JSON object in the response
            import re
            json_match = re.search(r'\{.*\}', llm_output, re.DOTALL)
            if json_match:
                llm_output = json_match.group(0)

        # 4. Parse and validate the response
        try:
            logger.info("Validating the generated STTM against the schema.")
            sttm = STTM.model_validate_json(llm_output)
            logger.info("STTM generation successful and validated.")
            return sttm
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM output as JSON. Error: {e}")
            raise STTMGenerationError("LLM output was not valid JSON.", llm_response=llm_output)
        except Exception as e:
            logger.error(f"Failed to validate LLM output against STTM schema. Error: {e}")
            raise STTMGenerationError("LLM output did not match STTM schema.", llm_response=llm_output)
