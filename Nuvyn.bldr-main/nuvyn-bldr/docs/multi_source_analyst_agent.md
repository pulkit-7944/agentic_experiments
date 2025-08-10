# Multi-Source Analyst Agent Documentation

## Overview

The Analyst Agent has been enhanced to support multiple data sources, including local files and Azure Storage Blob. This allows for flexible data ingestion from various sources without requiring manual data movement.

## 🚀 Key Features

### ✅ Supported Data Sources

1. **Local Files**
   - CSV files (`.csv`)
   - Excel files (`.xlsx`, `.xls`)
   - JSON files (`.json`)
   - Parquet files (`.parquet`)

2. **Azure Storage Blob**
   - All file formats supported by local files
   - Connection via connection string, account key, or SAS token
   - Support for both `azure://` protocol and full URLs

### 🔧 Architecture

The multi-source capability is built on a modular architecture:

```
AnalystAgent
├── DataSourceService
│   ├── LocalFileDataSource
│   └── AzureBlobDataSource
└── Profiler
```

## 📋 Configuration

### Environment Variables

Set these environment variables for Azure Storage access:

```bash
# Option 1: Connection String (recommended)
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=your_account;AccountKey=your_key;EndpointSuffix=core.windows.net"

# Option 2: Account Name + Key
export AZURE_STORAGE_ACCOUNT_NAME="your_account"
export AZURE_STORAGE_ACCOUNT_KEY="your_key"

# Option 3: SAS Token
export AZURE_STORAGE_SAS_TOKEN="?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupitx&se=2024-01-01T00:00:00Z&st=2023-01-01T00:00:00Z&spr=https&sig=your_signature"
```

### Configuration File

The settings are automatically loaded from the configuration:

```python
# src/nuvyn_bldr/core/config.py
AZURE_STORAGE_CONNECTION_STRING: Optional[str] = Field(None, description="Azure Storage connection string")
AZURE_STORAGE_ACCOUNT_NAME: Optional[str] = Field(None, description="Azure Storage account name")
AZURE_STORAGE_ACCOUNT_KEY: Optional[str] = Field(None, description="Azure Storage account key")
AZURE_STORAGE_SAS_TOKEN: Optional[str] = Field(None, description="Azure Storage SAS token")
```

## 🛠️ Installation

### Install Azure Dependencies

```bash
# Install Azure Storage dependencies
pip install -r requirements-azure.txt

# Or install individually
pip install azure-storage-blob azure-identity azure-core
```

### Optional Dependencies

For additional file format support:

```bash
# Excel support
pip install openpyxl xlrd

# Parquet support
pip install pyarrow
```

## 📖 Usage Examples

### 1. Local Files Only (Existing Functionality)

```bash
# Using CLI
python -m nuvyn_bldr run-analyst \
    --input-dir ./data \
    --output-path ./output/sttm.json

# Using Python
from nuvyn_bldr.agents.analyst.agent import AnalystAgent

analyst = AnalystAgent()
sttm_result = analyst.generate_sttm(file_paths=[
    "./data/customers.csv",
    "./data/orders.csv",
    "./data/products.csv"
])
```

### 2. Azure Blob Storage

```bash
# Using CLI with Azure credentials
python -m nuvyn_bldr run-analyst-azure \
    --source-paths \
        azure://mycontainer/data/customers.csv \
        azure://mycontainer/data/orders.csv \
        https://mystorage.blob.core.windows.net/mycontainer/data/products.csv \
    --output-path ./output/sttm.json \
    --azure-connection-string "your_connection_string"
```

### 3. Mixed Sources (Local + Azure)

```bash
# Using CLI with mixed sources
python -m nuvyn_bldr run-analyst-azure \
    --source-paths \
        ./data/local_customers.csv \
        azure://mycontainer/data/remote_orders.csv \
        ./data/local_products.csv \
    --output-path ./output/sttm.json \
    --azure-account-name "myaccount" \
    --azure-account-key "mykey"
```

### 4. Python API with Mixed Sources

```python
from nuvyn_bldr.agents.analyst.agent import AnalystAgent
from nuvyn_bldr.core.config import settings

# Configure Azure settings
settings.AZURE_STORAGE_CONNECTION_STRING = "your_connection_string"

# Create analyst agent
analyst = AnalystAgent()

# Generate STTM from mixed sources
sttm_result = analyst.generate_sttm(file_paths=[
    "./data/local_customers.csv",           # Local file
    "azure://mycontainer/remote_orders.csv", # Azure blob
    "./data/local_products.csv"             # Local file
])
```

## 🔍 Azure Path Formats

### Supported Formats

1. **Azure Protocol Format**
   ```
   azure://container/blob_name
   azure://mycontainer/data/customers.csv
   azure://raw-data/sales/orders.xlsx
   ```

2. **Full URL Format**
   ```
   https://account.blob.core.windows.net/container/blob_name
   https://mystorage.blob.core.windows.net/mycontainer/data/products.csv
   ```

### Path Examples

```python
# Valid Azure paths
azure_paths = [
    "azure://raw-data/customers.csv",
    "azure://processed-data/sales/orders.xlsx",
    "https://mystorage.blob.core.windows.net/raw-data/products.csv",
    "https://company-data.blob.core.windows.net/analytics/sales.json"
]

# Invalid paths (will be rejected)
invalid_paths = [
    "azure://",  # Missing container and blob
    "https://mystorage.blob.core.windows.net/",  # Missing container and blob
    "s3://bucket/file.csv",  # Unsupported protocol
    "gs://bucket/file.csv"   # Unsupported protocol
]
```

## 🔧 Data Source Service API

### Direct Usage

```python
from nuvyn_bldr.core.data_source_service import DataSourceService

# Initialize service
data_service = DataSourceService(
    azure_connection_string="your_connection_string"
)

# Validate source
if data_service.validate_source("azure://container/file.csv"):
    print("Source is supported")

# Read data
df = data_service.read_data("azure://container/file.csv")

# Get source name
source_name = data_service.get_source_name("azure://container/file.csv")
# Returns: "file.csv"

# List supported protocols
protocols = data_service.list_supported_protocols()
# Returns: ["local files", "azure blob storage"]
```

### Error Handling

```python
from nuvyn_bldr.core.data_source_service import DataSourceError

try:
    df = data_service.read_data("azure://container/file.csv")
except DataSourceError as e:
    print(f"Data source error: {e}")
    # Handle specific data source errors
except Exception as e:
    print(f"General error: {e}")
    # Handle other errors
```

## 🧪 Testing

### Run the Multi-Source Test

```bash
# Run comprehensive test
python test_multi_source_analyst.py
```

### Test Output

The test script will:

1. **Test Local Files**: Create sample data and test local file reading
2. **Test Azure Validation**: Test Azure path validation (without actual connection)
3. **Test Mixed Sources**: Test handling of mixed local and Azure sources
4. **Generate STTM**: Test STTM generation from multiple sources

### Expected Output

```
🚀 MULTI-SOURCE ANALYST AGENT TEST
============================================================

🧪 TESTING LOCAL FILES
============================================================
📁 Testing local file: /tmp/tmp123/customers.csv
✅ Source validated: /tmp/tmp123/customers.csv
📊 Data shape: (5, 4)
📋 Columns: ['customer_id', 'name', 'email', 'city']
🏷️  Source name: customers.csv

🤖 Testing Analyst Agent with local files...
✅ STTM generated successfully!
📋 Tables in STTM: 3
   - dim_customers (4 columns)
   - fact_orders (5 columns)
   - dim_products (4 columns)

☁️  TESTING AZURE BLOB (SIMULATED)
============================================================
🔍 Testing Azure path validation:
✅ Azure path valid: azure://mycontainer/data/customers.csv
   Source name: customers.csv
✅ Azure path valid: https://mystorage.blob.core.windows.net/mycontainer/data/orders.csv
   Source name: orders.csv

📋 Supported protocols:
   - local files
   - azure blob storage

✅ ALL TESTS COMPLETED!
```

## 🔒 Security Considerations

### Azure Authentication

1. **Connection String**: Most secure for production
2. **Account Key**: Good for development/testing
3. **SAS Token**: Limited time access, good for temporary access

### Best Practices

```bash
# Use environment variables (recommended)
export AZURE_STORAGE_CONNECTION_STRING="your_connection_string"

# Don't hardcode credentials in scripts
# ❌ Bad
python -m nuvyn_bldr run-analyst-azure \
    --azure-connection-string "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey"

# ✅ Good
python -m nuvyn_bldr run-analyst-azure \
    --source-paths azure://container/file.csv \
    --output-path ./output/sttm.json
```

## 🚀 Production Deployment

### Docker Configuration

```dockerfile
# Add Azure dependencies
RUN pip install azure-storage-blob azure-identity azure-core

# Set environment variables
ENV AZURE_STORAGE_CONNECTION_STRING="your_connection_string"
```

### Kubernetes Secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: azure-storage-secret
type: Opaque
data:
  connection-string: <base64-encoded-connection-string>
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nuvyn-bldr
spec:
  template:
    spec:
      containers:
      - name: nuvyn-bldr
        env:
        - name: AZURE_STORAGE_CONNECTION_STRING
          valueFrom:
            secretKeyRef:
              name: azure-storage-secret
              key: connection-string
```

## 🔄 Migration Guide

### From Local-Only to Multi-Source

1. **Install Dependencies**
   ```bash
   pip install -r requirements-azure.txt
   ```

2. **Update Configuration**
   ```python
   # Add Azure settings to your config
   settings.AZURE_STORAGE_CONNECTION_STRING = "your_connection_string"
   ```

3. **Update Code**
   ```python
   # Old way (local only)
   analyst.generate_sttm(file_paths=["./data/file.csv"])
   
   # New way (multi-source)
   analyst.generate_sttm(file_paths=[
       "./data/local_file.csv",
       "azure://container/remote_file.csv"
   ])
   ```

4. **Update CLI Commands**
   ```bash
   # Old way
   python -m nuvyn_bldr run-analyst --input-dir ./data --output-path ./output/sttm.json
   
   # New way (for mixed sources)
   python -m nuvyn_bldr run-analyst-azure \
       --source-paths ./data/local.csv azure://container/remote.csv \
       --output-path ./output/sttm.json
   ```

## 🐛 Troubleshooting

### Common Issues

1. **Import Error: No module named 'azure'**
   ```bash
   pip install azure-storage-blob
   ```

2. **Authentication Error**
   ```bash
   # Check credentials
   echo $AZURE_STORAGE_CONNECTION_STRING
   
   # Test connection
   python -c "from azure.storage.blob import BlobServiceClient; print('Connection OK')"
   ```

3. **Path Validation Error**
   ```python
   # Check path format
   data_service.validate_source("azure://container/file.csv")
   ```

4. **File Not Found**
   ```python
   # Check if blob exists
   blob_client = blob_service_client.get_blob_client(container="container", blob="file.csv")
   exists = blob_client.exists()
   ```

### Debug Mode

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# This will show detailed Azure operations
analyst = AnalystAgent()
sttm_result = analyst.generate_sttm(file_paths=["azure://container/file.csv"])
```

## 📚 Additional Resources

- [Azure Storage Blob Python SDK](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python)
- [Azure Storage Authentication](https://docs.microsoft.com/en-us/azure/storage/common/storage-auth)
- [Azure Storage Connection Strings](https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string) 