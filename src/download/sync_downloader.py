"""
Synchronous S3 Downloader - Basic boto3 downloader with retry logic

This module provides synchronous S3 downloads using boto3 with connection
pooling and exponential backoff retry.

Based on: pipeline_design/mac-optimized-pipeline.md
"""

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import gzip
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, Any
import time
import logging

from ..core.exceptions import S3DownloadError

logger = logging.getLogger(__name__)


class SyncS3Downloader:
    """
    Synchronous S3 downloader with retry and connection pooling

    Features:
    - Exponential backoff retry
    - Connection pooling
    - Automatic decompression
    - Progress tracking
    """

    def __init__(
        self,
        credentials: Dict[str, str],
        endpoint_url: str = 'https://files.polygon.io',
        max_retries: int = 5,
        timeout: int = 60,
        max_pool_connections: int = 10
    ):
        """
        Initialize S3 downloader

        Args:
            credentials: Dict with 'access_key_id' and 'secret_access_key'
            endpoint_url: S3 endpoint URL
            max_retries: Maximum retry attempts
            timeout: Request timeout in seconds
            max_pool_connections: Max connections in pool
        """
        self.endpoint_url = endpoint_url
        self.max_retries = max_retries
        self.timeout = timeout

        # Configure boto3 client
        config = Config(
            max_pool_connections=max_pool_connections,
            retries={
                'max_attempts': max_retries,
                'mode': 'adaptive'
            },
            connect_timeout=timeout,
            read_timeout=timeout,
        )

        # Create S3 client
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=credentials['access_key_id'],
            aws_secret_access_key=credentials['secret_access_key'],
            endpoint_url=endpoint_url,
            config=config
        )

        # Statistics
        self.download_count = 0
        self.retry_count = 0
        self.error_count = 0

        logger.info(f"SyncS3Downloader initialized (endpoint: {endpoint_url})")

    def download(
        self,
        bucket: str,
        key: str,
        decompress: bool = True
    ) -> BytesIO:
        """
        Download file from S3

        Args:
            bucket: S3 bucket name
            key: S3 object key
            decompress: Whether to decompress gzip files

        Returns:
            BytesIO object with file contents

        Raises:
            S3DownloadError: If download fails after retries
        """
        retry_delay = 1  # Initial delay in seconds

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug(f"Downloading s3://{bucket}/{key} (attempt {attempt})")

                # Download from S3
                response = self.s3.get_object(Bucket=bucket, Key=key)
                content = response['Body'].read()

                # Decompress if needed
                if decompress and key.endswith('.gz'):
                    content = gzip.decompress(content)

                self.download_count += 1

                logger.info(
                    f"Downloaded {key} "
                    f"({len(content) / 1024:.1f} KB, attempt {attempt})"
                )

                return BytesIO(content)

            except ClientError as e:
                error_code = e.response['Error']['Code']
                self.retry_count += 1

                if error_code == 'NoSuchKey':
                    # File doesn't exist, don't retry
                    self.error_count += 1
                    raise S3DownloadError(f"File not found: s3://{bucket}/{key}")

                elif error_code in ['SlowDown', 'RequestTimeout', '503']:
                    # Transient error, retry with backoff
                    if attempt < self.max_retries:
                        logger.warning(
                            f"Transient error ({error_code}), "
                            f"retrying in {retry_delay}s..."
                        )
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue
                    else:
                        self.error_count += 1
                        raise S3DownloadError(
                            f"Download failed after {self.max_retries} attempts: {e}"
                        )

                else:
                    # Unknown error
                    self.error_count += 1
                    raise S3DownloadError(f"Download error: {e}")

            except Exception as e:
                self.error_count += 1
                raise S3DownloadError(f"Unexpected error: {e}")

        # Should not reach here
        raise S3DownloadError("Download failed")

    def download_to_file(
        self,
        bucket: str,
        key: str,
        local_path: Path,
        decompress: bool = True
    ):
        """
        Download file from S3 and save to disk

        Args:
            bucket: S3 bucket name
            key: S3 object key
            local_path: Local file path to save
            decompress: Whether to decompress gzip files
        """
        content = self.download(bucket, key, decompress=decompress)

        # Create parent directory
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to file
        with open(local_path, 'wb') as f:
            f.write(content.getvalue())

        logger.info(f"Saved to {local_path}")

    def list_objects(
        self,
        bucket: str,
        prefix: str,
        max_keys: int = 1000
    ) -> list:
        """
        List objects in S3 bucket with prefix

        Args:
            bucket: S3 bucket name
            prefix: Key prefix to filter
            max_keys: Maximum keys to return

        Returns:
            List of object keys
        """
        try:
            response = self.s3.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=max_keys
            )

            if 'Contents' not in response:
                return []

            keys = [obj['Key'] for obj in response['Contents']]
            logger.info(f"Listed {len(keys)} objects with prefix {prefix}")
            return keys

        except Exception as e:
            raise S3DownloadError(f"List objects failed: {e}")

    def check_exists(self, bucket: str, key: str) -> bool:
        """
        Check if object exists in S3

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            True if exists, False otherwise
        """
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise S3DownloadError(f"Check exists failed: {e}")

    def get_statistics(self) -> Dict[str, int]:
        """
        Get download statistics

        Returns:
            Dictionary with statistics
        """
        return {
            'download_count': self.download_count,
            'retry_count': self.retry_count,
            'error_count': self.error_count,
            'success_rate': (
                self.download_count / (self.download_count + self.error_count)
                if (self.download_count + self.error_count) > 0
                else 0.0
            )
        }

    def reset_statistics(self):
        """Reset statistics counters"""
        self.download_count = 0
        self.retry_count = 0
        self.error_count = 0


def main():
    """Command-line interface for sync downloader"""
    import sys
    from ..core.config_loader import ConfigLoader

    # Load configuration
    try:
        config = ConfigLoader()
        credentials = config.get_credentials('polygon')

        if not credentials or 's3' not in credentials:
            print("❌ Credentials not found. Please configure config/credentials.yaml")
            sys.exit(1)

        s3_creds = credentials['s3']

        # Create downloader
        downloader = SyncS3Downloader(
            credentials={
                'access_key_id': s3_creds['access_key_id'],
                'secret_access_key': s3_creds['secret_access_key'],
            },
            endpoint_url=s3_creds.get('endpoint_url', 'https://files.polygon.io')
        )

        print("✅ SyncS3Downloader initialized")
        print(f"   Endpoint: {downloader.endpoint_url}")
        print(f"   Max retries: {downloader.max_retries}")

        # Test: List objects
        print("\n📋 Testing list_objects...")
        bucket = s3_creds.get('bucket', 'flatfiles')
        prefix = 'us_stocks_sip/day_aggs_v1/2025/09/'

        keys = downloader.list_objects(bucket, prefix, max_keys=5)
        print(f"   Found {len(keys)} files:")
        for key in keys[:5]:
            print(f"     - {key}")

        # Statistics
        stats = downloader.get_statistics()
        print(f"\n📊 Statistics:")
        print(f"   Downloads: {stats['download_count']}")
        print(f"   Retries: {stats['retry_count']}")
        print(f"   Errors: {stats['error_count']}")

    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
