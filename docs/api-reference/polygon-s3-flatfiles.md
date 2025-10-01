# Polygon.io S3 Flat Files Download

This document provides Python scripts to download various flat file data from Polygon.io's S3-compatible storage.

## Prerequisites

```bash
pip install boto3
```

## Credentials

- **Access Key ID**: `93c70b3c-17c1-43f8-b180-f1be27cec67b`
- **Secret Access Key**: `vDr8GDaQ87Z9Mwe5IiCKzGcRP9pnO8TW`
- **Endpoint URL**: `https://files.polygon.io`
- **Bucket Name**: `flatfiles`

## Available Data Types

### 1. US Stocks SIP - Daily Aggregates

Downloads daily aggregate data for US stocks.

```python
import boto3
from botocore.config import Config

# Initialize a session using your credentials
session = boto3.Session(
  aws_access_key_id='93c70b3c-17c1-43f8-b180-f1be27cec67b',
  aws_secret_access_key='vDr8GDaQ87Z9Mwe5IiCKzGcRP9pnO8TW',
)

# Create a client with your session and specify the endpoint
s3 = session.client(
  's3',
  endpoint_url='https://files.polygon.io',
  config=Config(signature_version='s3v4'),
)

# Specify the bucket name
bucket_name = 'flatfiles'

# Specify the S3 object key name
object_key = 'flatfiles/us_stocks_sip/day_aggs_v1/2025/09/2025-09-29.csv.gz'

# Remove the bucket name (e.g. 'flatfiles/') prefix if present in object_key
if object_key.startswith(bucket_name + '/'):
  object_key = object_key[len(bucket_name + '/'):]

# Specify the local file name and path to save the downloaded file
local_file_name = object_key.split('/')[-1]  # e.g., '2025-06-12.csv.gz'
local_file_path = './' + local_file_name

# Print the file being downloaded
print(f"Downloading file '{object_key}' from bucket '{bucket_name}'...")

# Download the file
s3.download_file(bucket_name, object_key, local_file_path)
```

### 2. US Stocks SIP - Minute Aggregates

Downloads minute-level aggregate data for US stocks.

```python
import boto3
from botocore.config import Config

# Initialize a session using your credentials
session = boto3.Session(
  aws_access_key_id='93c70b3c-17c1-43f8-b180-f1be27cec67b',
  aws_secret_access_key='vDr8GDaQ87Z9Mwe5IiCKzGcRP9pnO8TW',
)

# Create a client with your session and specify the endpoint
s3 = session.client(
  's3',
  endpoint_url='https://files.polygon.io',
  config=Config(signature_version='s3v4'),
)

# Specify the bucket name
bucket_name = 'flatfiles'

# Specify the S3 object key name
object_key = 'flatfiles/us_stocks_sip/minute_aggs_v1/2025/09/2025-09-29.csv.gz'

# Remove the bucket name (e.g. 'flatfiles/') prefix if present in object_key
if object_key.startswith(bucket_name + '/'):
  object_key = object_key[len(bucket_name + '/'):]

# Specify the local file name and path to save the downloaded file
local_file_name = object_key.split('/')[-1]  # e.g., '2025-06-12.csv.gz'
local_file_path = './' + local_file_name

# Print the file being downloaded
print(f"Downloading file '{object_key}' from bucket '{bucket_name}'...")

# Download the file
s3.download_file(bucket_name, object_key, local_file_path)
```

### 3. US Options OPRA - Daily Aggregates

Downloads daily aggregate data for US options.

```python
import boto3
from botocore.config import Config

# Initialize a session using your credentials
session = boto3.Session(
  aws_access_key_id='93c70b3c-17c1-43f8-b180-f1be27cec67b',
  aws_secret_access_key='vDr8GDaQ87Z9Mwe5IiCKzGcRP9pnO8TW',
)

# Create a client with your session and specify the endpoint
s3 = session.client(
  's3',
  endpoint_url='https://files.polygon.io',
  config=Config(signature_version='s3v4'),
)

# Specify the bucket name
bucket_name = 'flatfiles'

# Specify the S3 object key name
object_key = 'flatfiles/us_options_opra/day_aggs_v1/2025/09/2025-09-29.csv.gz'

# Remove the bucket name (e.g. 'flatfiles/') prefix if present in object_key
if object_key.startswith(bucket_name + '/'):
  object_key = object_key[len(bucket_name + '/'):]

# Specify the local file name and path to save the downloaded file
local_file_name = object_key.split('/')[-1]  # e.g., '2025-06-12.csv.gz'
local_file_path = './' + local_file_name

# Print the file being downloaded
print(f"Downloading file '{object_key}' from bucket '{bucket_name}'...")

# Download the file
s3.download_file(bucket_name, object_key, local_file_path)
```

### 4. US Options OPRA - Minute Aggregates

Downloads minute-level aggregate data for US options.

```python
import boto3
from botocore.config import Config

# Initialize a session using your credentials
session = boto3.Session(
  aws_access_key_id='93c70b3c-17c1-43f8-b180-f1be27cec67b',
  aws_secret_access_key='vDr8GDaQ87Z9Mwe5IiCKzGcRP9pnO8TW',
)

# Create a client with your session and specify the endpoint
s3 = session.client(
  's3',
  endpoint_url='https://files.polygon.io',
  config=Config(signature_version='s3v4'),
)

# Specify the bucket name
bucket_name = 'flatfiles'

# Specify the S3 object key name
object_key = 'flatfiles/us_options_opra/minute_aggs_v1/2025/09/2025-09-29.csv.gz'

# Remove the bucket name (e.g. 'flatfiles/') prefix if present in object_key
if object_key.startswith(bucket_name + '/'):
  object_key = object_key[len(bucket_name + '/'):]

# Specify the local file name and path to save the downloaded file
local_file_name = object_key.split('/')[-1]  # e.g., '2025-06-12.csv.gz'
local_file_path = './' + local_file_name

# Print the file being downloaded
print(f"Downloading file '{object_key}' from bucket '{bucket_name}'...")

# Download the file
s3.download_file(bucket_name, object_key, local_file_path)
```

## File Path Structure

All files follow this pattern:
```
flatfiles/{data_type}/{agg_type}/{year}/{month}/{date}.csv.gz
```

**Examples:**
- `flatfiles/us_stocks_sip/day_aggs_v1/2025/09/2025-09-29.csv.gz`
- `flatfiles/us_stocks_sip/minute_aggs_v1/2025/09/2025-09-29.csv.gz`
- `flatfiles/us_options_opra/day_aggs_v1/2025/09/2025-09-29.csv.gz`
- `flatfiles/us_options_opra/minute_aggs_v1/2025/09/2025-09-29.csv.gz`

## Usage Notes

1. Files are compressed as `.csv.gz` format
2. The script automatically extracts the filename from the object key
3. Files are downloaded to the current working directory by default
4. Modify `object_key` to download different dates or data types
5. The script handles the bucket name prefix removal automatically
