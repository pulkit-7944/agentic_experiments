# src/nuvyn_bldr/core/profiling.py

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
import logging
import os
from pydantic import BaseModel

# Set up logging for this module
logger = logging.getLogger(__name__)

def _profile_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generates a detailed profile of a single pandas DataFrame.

    Args:
        df: The DataFrame to profile.

    Returns:
        A dictionary containing the profile information.
    """
    profile = {
        "record_count": len(df),
        "columns": []
    }

    for col in df.columns:
        column_profile = {
            "name": col,
            "dtype": str(df[col].dtype),
            "null_count": int(df[col].isnull().sum()),
            "null_percentage": round((df[col].isnull().sum() / len(df)) * 100, 2) if len(df) > 0 else 0,
            "unique_count": int(df[col].nunique()),
        }
        
        # Add specific stats for numeric and object types
        if pd.api.types.is_numeric_dtype(df[col].dtype):
            stats = df[col].describe()
            column_profile["stats"] = {
                "mean": stats.get("mean", 0),
                "std": stats.get("std", 0),
                "min": stats.get("min", 0),
                "25%": stats.get("25%", 0),
                "50%": stats.get("50%", 0),
                "75%": stats.get("75%", 0),
                "max": stats.get("max", 0),
            }
        
        profile["columns"].append(column_profile)
        
    return profile

def profile_dataset(
    file_paths: List[str], 
    sampling_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Profiles a dataset consisting of one or more CSV files.

    This function can apply sampling strategies to handle large files before
    generating a detailed profile for each file.

    Args:
        file_paths: A list of full paths to the CSV files.
        sampling_config: An optional dictionary to configure sampling.
            Examples:
            - {"strategy": "top_n", "value": 10000}
            - {"strategy": "random_percent", "value": 0.1}
            If None, the entire file is processed.

    Returns:
        A dictionary where keys are the filenames and values are the
        detailed profile reports for each file.
    """
    logger.info(f"Starting dataset profiling for {len(file_paths)} files.")
    if sampling_config:
        logger.info(f"Applying sampling strategy: {sampling_config}")

    dataset_profile = {}

    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        logger.info(f"Processing file: {file_name}")

        try:
            if not sampling_config:
                # No sampling, read the whole file
                df = pd.read_csv(file_path)
            else:
                strategy = sampling_config.get("strategy")
                value = sampling_config.get("value")

                if strategy == "top_n":
                    df = pd.read_csv(file_path, nrows=value)
                elif strategy == "random_percent":
                    # Read a fraction of rows, requires skipping rows randomly
                    df = pd.read_csv(
                        file_path,
                        skiprows=lambda i: i > 0 and np.random.rand() > value
                    )
                else:
                    logger.warning(f"Unknown sampling strategy '{strategy}'. Reading full file.")
                    df = pd.read_csv(file_path)
            
            # Generate the profile for the (potentially sampled) DataFrame
            file_profile = _profile_dataframe(df)
            dataset_profile[file_name] = file_profile
            logger.info(f"Successfully profiled {file_name}.")

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Skipping.")
            dataset_profile[file_name] = {"error": "File not found"}
        except Exception as e:
            logger.error(f"An error occurred while processing {file_path}: {e}", exc_info=True)
            dataset_profile[file_name] = {"error": str(e)}
            
    logger.info("Dataset profiling complete.")
    return dataset_profile

class Profiler:
    """
    Profiler class to encapsulate dataset profiling behavior.
    This wraps around the `profile_dataset` function.
    """
    def __init__(self, sampling_config: Optional[Dict[str, Any]] = None):
        self.sampling_config = sampling_config

    def run(self, file_paths: List[str]) -> Dict[str, Any]:
        """
        Profiles the dataset based on the provided file paths and sampling config.

        Args:
            file_paths: List of CSV file paths to be profiled.

        Returns:
            Dictionary of profiling results.
        """
        return profile_dataset(file_paths, self.sampling_config)
    
    def profile_dataframe(self, df: pd.DataFrame, source_name: str, sampler_config: 'SamplerConfig') -> 'TableProfile':
        """
        Profiles a single DataFrame and returns a TableProfile object.

        Args:
            df: The DataFrame to profile
            source_name: Name of the data source
            sampler_config: Sampling configuration

        Returns:
            TableProfile object containing the profile data
        """
        # Apply sampling if configured
        if sampler_config:
            if sampler_config.strategy == "top_n":
                df = df.head(int(sampler_config.value))
            elif sampler_config.strategy == "random_percent":
                df = df.sample(frac=sampler_config.value)
        
        # Generate profile
        profile_data = _profile_dataframe(df)
        profile_data["source_name"] = source_name
        
        return TableProfile(profile_data=profile_data)
    
class SamplerConfig:
    def __init__(self, strategy: str, value: float):
        self.strategy = strategy
        self.value = value

    def to_dict(self):
        return {"strategy": self.strategy, "value": self.value}

from pydantic import BaseModel

class TableProfile(BaseModel):
    profile_data: dict
