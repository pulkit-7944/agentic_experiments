# src/nuvyn_bldr/agents/analyst/tools.py

from langchain_core.tools import tool
from typing import List, Dict, Any, Optional
from nuvyn_bldr.core.profiling import profile_dataset as core_profile_dataset
import logging

logger = logging.getLogger(__name__)

@tool
def profiling_tool(
    file_paths: List[str], 
    sampling_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Analyzes and profiles a list of dataset files to understand their structure.

    This is the primary tool to use when you need to understand the content of
    the source data. It provides critical information like column names, data types,
    null percentages, and uniqueness, which is essential for designing a data warehouse.
    You can optionally provide a sampling configuration to process large files
    more efficiently. If you don't provide a sampling configuration, the entire
    file will be read.

    Args:
        file_paths: A list of full paths to the CSV files to be profiled.
        sampling_config: An optional dictionary to configure sampling.
            Valid strategies are 'top_n' (e.g., {"strategy": "top_n", "value": 10000})
            or 'random_percent' (e.g., {"strategy": "random_percent", "value": 0.1}).
    """
    logger.info(f"Executing profiling_tool for {len(file_paths)} files.")
    return core_profile_dataset(file_paths=file_paths, sampling_config=sampling_config)

# A list of all tools available to the Analyst Agent.
# The agent will be given this list to choose from.
analyst_tools = [profiling_tool]