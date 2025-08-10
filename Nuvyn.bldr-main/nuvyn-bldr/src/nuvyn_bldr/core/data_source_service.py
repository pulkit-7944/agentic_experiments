# src/nuvyn_bldr/core/data_source_service.py

import pandas as pd
import logging
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
from abc import ABC, abstractmethod
import re
import io
import subprocess
import tempfile
from .sampling_service import SamplingService

logger = logging.getLogger(__name__)

class DataSourceError(Exception):
    """Custom exception for data source related errors."""
    pass

class DataSource(ABC):
    """Abstract base class for data sources."""
    
    @abstractmethod
    def can_handle(self, source_path: str) -> bool:
        """Check if this data source can handle the given path."""
        pass
    
    @abstractmethod
    def read_data(self, source_path: str) -> pd.DataFrame:
        """Read data from the source and return a pandas DataFrame."""
        pass
    
    @abstractmethod
    def get_source_name(self, source_path: str) -> str:
        """Extract a meaningful source name from the path."""
        pass

class LocalFileDataSource(DataSource):
    """Handles local file system data sources."""
    
    def can_handle(self, source_path: str) -> bool:
        """Check if this is a local file path."""
        # Check if it's a local file path (not starting with protocols)
        return not source_path.startswith(('http://', 'https://', 'azure://', 's3://', 'gs://'))
    
    def read_data(self, source_path: str) -> pd.DataFrame:
        """Read data from local file."""
        try:
            path = Path(source_path)
            if not path.exists():
                raise DataSourceError(f"File not found: {source_path}")
            
            # Initialize sampling service
            sampling_service = SamplingService()
            
            # Determine file type and read accordingly
            if path.suffix.lower() == '.csv':
                # Check if we should sample this file
                if sampling_service.should_sample_file(source_path):
                    logger.info(f"Large CSV file detected, applying sampling: {source_path}")
                    return sampling_service.sample_large_csv(source_path)
                else:
                    return pd.read_csv(source_path)
            elif path.suffix.lower() in ['.xlsx', '.xls']:
                return pd.read_excel(source_path)
            elif path.suffix.lower() == '.json':
                return pd.read_json(source_path)
            elif path.suffix.lower() == '.parquet':
                return pd.read_parquet(source_path)
            else:
                raise DataSourceError(f"Unsupported file type: {path.suffix}")
                
        except Exception as e:
            raise DataSourceError(f"Failed to read local file {source_path}: {str(e)}")
    
    def get_source_name(self, source_path: str) -> str:
        """Extract filename from local path."""
        return Path(source_path).name

class AzureBlobDataSource(DataSource):
    """Handles Azure Storage Blob data sources."""
    
    def __init__(self, connection_string: Optional[str] = None, account_name: Optional[str] = None, 
                 account_key: Optional[str] = None, sas_token: Optional[str] = None):
        """
        Initialize Azure Blob data source.
        
        Args:
            connection_string: Azure Storage connection string
            account_name: Azure Storage account name
            account_key: Azure Storage account key
            sas_token: Shared Access Signature token
        """
        self.connection_string = connection_string
        self.account_name = account_name
        self.account_key = account_key
        self.sas_token = sas_token
        self._blob_service_client = None
    
    def can_handle(self, source_path: str) -> bool:
        """Check if this is an Azure blob path."""
        return source_path.startswith('azure://') or 'blob.core.windows.net' in source_path
    
    def _get_blob_service_client(self):
        """Get or create Azure Blob Service Client."""
        if self._blob_service_client is None:
            try:
                from azure.storage.blob import BlobServiceClient
                
                if self.connection_string:
                    self._blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
                elif self.account_name and (self.account_key or self.sas_token):
                    account_url = f"https://{self.account_name}.blob.core.windows.net"
                    if self.account_key:
                        from azure.storage.blob import BlobServiceClient
                        self._blob_service_client = BlobServiceClient(account_url=account_url, credential=self.account_key)
                    elif self.sas_token:
                        from azure.storage.blob import BlobServiceClient
                        self._blob_service_client = BlobServiceClient(account_url=account_url, credential=self.sas_token)
                else:
                    raise DataSourceError("Azure credentials not provided")
                    
            except ImportError:
                raise DataSourceError("azure-storage-blob package not installed. Install with: pip install azure-storage-blob")
            except Exception as e:
                raise DataSourceError(f"Failed to initialize Azure Blob Service Client: {str(e)}")
        
        return self._blob_service_client
    
    def _parse_azure_path(self, source_path: str) -> Dict[str, str]:
        """Parse Azure blob path to extract container and blob name."""
        # Handle different Azure path formats
        if source_path.startswith('azure://'):
            # Format: azure://container/blob_name
            path_parts = source_path[8:].split('/', 1)
            if len(path_parts) != 2:
                raise DataSourceError(f"Invalid Azure path format: {source_path}")
            return {'container': path_parts[0], 'blob_name': path_parts[1]}
        elif 'blob.core.windows.net' in source_path:
            # Format: https://account.blob.core.windows.net/container/blob_name?SAS_TOKEN
            # Remove query parameters for path parsing
            base_url = source_path.split('?')[0]
            match = re.search(r'https://([^.]+)\.blob\.core\.windows\.net/([^/]+)/(.+)', base_url)
            if not match:
                raise DataSourceError(f"Invalid Azure blob URL format: {source_path}")
            
            # Extract account name, container and blob name
            account_name = match.group(1)
            container = match.group(2)
            blob_name = match.group(3)
            
            # If there are query parameters, they should be SAS token
            if '?' in source_path:
                query_params = source_path.split('?', 1)[1]
                return {
                    'account_name': account_name,
                    'container': container, 
                    'blob_name': blob_name,
                    'sas_token': query_params
                }
            else:
                return {
                    'account_name': account_name,
                    'container': container, 
                    'blob_name': blob_name
                }
        else:
            raise DataSourceError(f"Unsupported Azure path format: {source_path}")
    
    def read_data(self, source_path: str) -> pd.DataFrame:
        """Read data from Azure blob."""
        try:
            path_info = self._parse_azure_path(source_path)
            
            # If we have a SAS token in the URL, use it directly
            if 'sas_token' in path_info:
                # Construct the full URL with SAS token
                account_name = path_info.get('account_name', 'nuvynragstorage')  # Default from URL
                full_url = f"https://{account_name}.blob.core.windows.net/{path_info['container']}/{path_info['blob_name']}?{path_info['sas_token']}"
                
                # Read directly from URL with sampling for large files
                blob_name = path_info['blob_name'].lower()
                
                if blob_name.endswith('.csv'):
                    # For large Azure blobs, use efficient streaming sampling
                    logger.info("Large Azure blob detected, using efficient streaming sampling")
                    
                    try:
                        # Use a single pass to sample from different parts of the file
                        sampled_chunks = []
                        target_sample_size = 1000
                        chunk_size = 10000  # Read in 10K chunks
                        max_chunks = 10  # Sample from up to 10 chunks
                        
                        logger.info(f"Sampling {target_sample_size} rows using chunk size: {chunk_size}")
                        
                        for i, chunk in enumerate(pd.read_csv(full_url, chunksize=chunk_size)):
                            if len(sampled_chunks) >= max_chunks:
                                break
                            
                            # Sample from this chunk (beginning, middle, end)
                            rows_per_chunk = target_sample_size // max_chunks
                            if len(chunk) > rows_per_chunk:
                                # Take samples from beginning, middle, and end of chunk
                                start_sample = chunk.head(rows_per_chunk // 3)
                                middle_start = len(chunk) // 2 - rows_per_chunk // 6
                                middle_end = len(chunk) // 2 + rows_per_chunk // 6
                                middle_sample = chunk.iloc[max(0, middle_start):middle_end]
                                end_sample = chunk.tail(rows_per_chunk // 3)
                                
                                # Combine samples from this chunk
                                chunk_sample = pd.concat([start_sample, middle_sample, end_sample], ignore_index=True)
                                sampled_chunks.append(chunk_sample)
                            else:
                                sampled_chunks.append(chunk)
                        
                        # Combine all samples
                        if sampled_chunks:
                            df = pd.concat(sampled_chunks, ignore_index=True)
                            
                            # Ensure we don't exceed target size
                            if len(df) > target_sample_size:
                                df = df.head(target_sample_size)
                            
                            # Limit columns if needed
                            if len(df.columns) > 50:
                                logger.info(f"Limiting columns from {len(df.columns)} to 50")
                                df = df.iloc[:, :50]
                            
                            logger.info(f"Successfully sampled Azure blob: {len(df)} rows, {len(df.columns)} columns")
                            logger.info(f"Sample represents data from {len(sampled_chunks)} different sections of the file")
                            return df
                        else:
                            # Fallback to simple sampling
                            logger.info("No chunks sampled, using fallback sampling")
                            return pd.read_csv(full_url, nrows=1000)
                        
                    except Exception as e:
                        logger.error(f"Efficient streaming sampling failed: {e}")
                        # Fallback to simple sampling
                        logger.info("Using fallback sampling due to error")
                        return pd.read_csv(full_url, nrows=1000)
                        
                elif blob_name.endswith(('.xlsx', '.xls')):
                    return pd.read_excel(full_url)
                elif blob_name.endswith('.json'):
                    return pd.read_json(full_url)
                elif blob_name.endswith('.parquet'):
                    return pd.read_parquet(full_url)
                else:
                    raise DataSourceError(f"Unsupported file type: {blob_name}")
            else:
                # Use blob service client (for connection string/auth)
                blob_service_client = self._get_blob_service_client()
                
                # Get blob client
                blob_client = blob_service_client.get_blob_client(
                    container=path_info['container'], 
                    blob=path_info['blob_name']
                )
                
                # Download blob content
                blob_data = blob_client.download_blob()
                
                # Convert bytes to string for pandas
                import io
                content = blob_data.readall()
                
                # Determine file type and read accordingly
                blob_name = path_info['blob_name'].lower()
                
                if blob_name.endswith('.csv'):
                    # Initialize sampling service
                    sampling_service = SamplingService()
                    
                    # Try to read with sampling for large files
                    try:
                        df = pd.read_csv(io.BytesIO(content))
                        
                        # Apply sampling if needed
                        if len(df) > sampling_service.max_rows:
                            logger.info(f"Large file detected ({len(df)} rows). Applying sampling.")
                            df = df.head(sampling_service.max_rows)
                        
                        # Limit columns if needed
                        if len(df.columns) > sampling_service.max_columns:
                            logger.info(f"Large file detected ({len(df.columns)} columns). Limiting to first {sampling_service.max_columns} columns.")
                            df = df.iloc[:, :sampling_service.max_columns]
                        
                        return df
                    except Exception as e:
                        logger.warning(f"Error reading full file, trying with limited rows: {e}")
                        return pd.read_csv(io.BytesIO(content), nrows=sampling_service.max_rows)
                        
                elif blob_name.endswith(('.xlsx', '.xls')):
                    return pd.read_excel(io.BytesIO(content))
                elif blob_name.endswith('.json'):
                    return pd.read_json(io.BytesIO(content))
                elif blob_name.endswith('.parquet'):
                    return pd.read_parquet(io.BytesIO(content))
                else:
                    raise DataSourceError(f"Unsupported file type: {blob_name}")
                
        except Exception as e:
            raise DataSourceError(f"Failed to read Azure blob {source_path}: {str(e)}")
    
    def get_source_name(self, source_path: str) -> str:
        """Extract blob name from Azure path."""
        try:
            path_info = self._parse_azure_path(source_path)
            return Path(path_info['blob_name']).name
        except:
            return source_path.split('/')[-1]

class DataSourceService:
    """Service for handling multiple data sources."""
    
    def __init__(self, azure_connection_string: Optional[str] = None, 
                 azure_account_name: Optional[str] = None,
                 azure_account_key: Optional[str] = None,
                 azure_sas_token: Optional[str] = None):
        """
        Initialize the data source service.
        
        Args:
            azure_connection_string: Azure Storage connection string
            azure_account_name: Azure Storage account name
            azure_account_key: Azure Storage account key
            azure_sas_token: Shared Access Signature token
        """
        self.data_sources = [
            LocalFileDataSource(),
            AzureBlobDataSource(
                connection_string=azure_connection_string,
                account_name=azure_account_name,
                account_key=azure_account_key,
                sas_token=azure_sas_token
            )
        ]
    
    def get_data_source(self, source_path: str) -> DataSource:
        """Get the appropriate data source for the given path."""
        for data_source in self.data_sources:
            if data_source.can_handle(source_path):
                return data_source
        
        raise DataSourceError(f"No data source found to handle: {source_path}")
    
    def read_data(self, source_path: str) -> pd.DataFrame:
        """Read data from any supported data source."""
        data_source = self.get_data_source(source_path)
        return data_source.read_data(source_path)
    
    def get_source_name(self, source_path: str) -> str:
        """Get source name from any supported data source."""
        data_source = self.get_data_source(source_path)
        return data_source.get_source_name(source_path)
    
    def validate_source(self, source_path: str) -> bool:
        """Validate if a source path can be handled."""
        try:
            self.get_data_source(source_path)
            return True
        except DataSourceError:
            return False
    
    def list_supported_protocols(self) -> List[str]:
        """List all supported data source protocols."""
        protocols = []
        for data_source in self.data_sources:
            if isinstance(data_source, LocalFileDataSource):
                protocols.append("local files")
            elif isinstance(data_source, AzureBlobDataSource):
                protocols.append("azure blob storage")
        return protocols 