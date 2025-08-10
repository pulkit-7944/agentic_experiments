#!/usr/bin/env python3
"""
Sampling Service for handling large datasets efficiently
"""

import os
import pandas as pd
import subprocess
import tempfile
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class SamplingService:
    """Service for intelligent data sampling from various sources"""
    
    def __init__(self, max_rows: int = 1000, max_columns: int = 50):
        self.max_rows = max_rows
        self.max_columns = max_columns
    
    def sample_large_csv(self, file_path: str) -> pd.DataFrame:
        """Sample large CSV files using comprehensive sampling strategy"""
        logger.info(f"Comprehensive sampling of large CSV: {file_path}")
        
        try:
            # Get total file size and estimate total rows
            file_size = os.path.getsize(file_path)
            logger.info(f"File size: {file_size / (1024*1024):.2f} MB")
            
            # Read in chunks to get total row count and sample from different parts
            total_rows = 0
            chunks = []
            
            # First pass: count total rows
            logger.info("Counting total rows...")
            for chunk in pd.read_csv(file_path, chunksize=10000):
                total_rows += len(chunk)
            
            logger.info(f"Total rows in file: {total_rows}")
            
            # Calculate sampling strategy
            sample_size = min(self.max_rows, total_rows)
            chunk_size = max(1000, total_rows // 10)  # Read in 10 chunks
            
            logger.info(f"Sampling {sample_size} rows from {total_rows} total rows")
            logger.info(f"Using chunk size: {chunk_size}")
            
            # Second pass: sample from different parts of the file
            sampled_chunks = []
            rows_per_chunk = sample_size // 10  # Distribute samples across 10 sections
            
            for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
                if len(sampled_chunks) >= 10:  # We have enough chunks
                    break
                
                # Sample from this chunk
                if len(chunk) > rows_per_chunk:
                    # Take samples from beginning, middle, and end of chunk
                    start_sample = chunk.head(rows_per_chunk // 3)
                    middle_sample = chunk.iloc[len(chunk)//2 - rows_per_chunk//6:len(chunk)//2 + rows_per_chunk//6]
                    end_sample = chunk.tail(rows_per_chunk // 3)
                    
                    # Combine samples from this chunk
                    chunk_sample = pd.concat([start_sample, middle_sample, end_sample], ignore_index=True)
                    sampled_chunks.append(chunk_sample)
                else:
                    sampled_chunks.append(chunk)
            
            # Combine all sampled chunks
            if sampled_chunks:
                df = pd.concat(sampled_chunks, ignore_index=True)
                
                # Ensure we don't exceed max_rows
                if len(df) > self.max_rows:
                    df = df.head(self.max_rows)
                
                # Limit columns if needed
                if len(df.columns) > self.max_columns:
                    logger.info(f"Limiting columns from {len(df.columns)} to {self.max_columns}")
                    df = df.iloc[:, :self.max_columns]
                
                logger.info(f"Successfully sampled: {len(df)} rows, {len(df.columns)} columns")
                logger.info(f"Sample represents {len(df)/total_rows*100:.1f}% of the original data")
                return df
            else:
                raise Exception("No data could be sampled from file")
                
        except Exception as e:
            logger.error(f"Comprehensive sampling failed: {e}")
            # Fallback to simple head sampling
            return self._fallback_sampling(file_path)
    
    def _fallback_sampling(self, file_path: str) -> pd.DataFrame:
        """Fallback sampling using head command"""
        logger.info("Using fallback sampling method")
        
        try:
            # Use head command to get first N rows
            head_cmd = f"head -n {self.max_rows + 1} '{file_path}'"
            result = subprocess.run(head_cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                # Create temporary file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                    temp_file.write(result.stdout)
                    temp_file_path = temp_file.name
                
                try:
                    df = pd.read_csv(temp_file_path)
                    
                    # Limit columns if needed
                    if len(df.columns) > self.max_columns:
                        logger.info(f"Limiting columns from {len(df.columns)} to {self.max_columns}")
                        df = df.iloc[:, :self.max_columns]
                    
                    logger.info(f"Fallback sampling successful: {len(df)} rows, {len(df.columns)} columns")
                    return df
                    
                finally:
                    os.unlink(temp_file_path)
            else:
                raise Exception(f"Head command failed: {result.stderr}")
                
        except Exception as e:
            logger.error(f"Fallback sampling failed: {e}")
            raise
    
    def should_sample_file(self, file_path: str) -> bool:
        """Check if file should be sampled based on size"""
        try:
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            return size_mb > 100  # Sample if > 100MB
        except:
            return False
    
    def sample_azure_blob(self, blob_url: str) -> pd.DataFrame:
        """Sample Azure blob using command-line tools"""
        logger.info(f"Sampling Azure blob: {blob_url}")
        
        try:
            # Extract blob name from URL
            blob_name = blob_url.split('/')[-1].split('?')[0]
            
            # Use Azure CLI to get first N lines
            cmd = [
                "az", "storage", "blob", "download",
                "--account-name", "nuvynragstorage",
                "--container-name", "sampledata",
                "--name", blob_name,
                "--file", "-",  # Output to stdout
                "--query", "content",
                "--output", "tsv"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(f"Failed to download blob: {result.stderr}")
            
            # Use head command to limit lines
            head_cmd = f"echo '{result.stdout}' | head -n {self.max_rows + 1}"
            head_result = subprocess.run(head_cmd, shell=True, capture_output=True, text=True)
            
            if head_result.returncode == 0:
                # Create temporary file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                    temp_file.write(head_result.stdout)
                    temp_file_path = temp_file.name
                
                try:
                    df = pd.read_csv(temp_file_path)
                    
                    # Limit columns if needed
                    if len(df.columns) > self.max_columns:
                        logger.info(f"Limiting columns from {len(df.columns)} to {self.max_columns}")
                        df = df.iloc[:, :self.max_columns]
                    
                    logger.info(f"Successfully sampled Azure blob: {len(df)} rows, {len(df.columns)} columns")
                    return df
                    
                finally:
                    os.unlink(temp_file_path)
            else:
                raise Exception(f"Head command failed: {head_result.stderr}")
                
        except Exception as e:
            logger.error(f"Azure blob sampling failed: {e}")
            raise 