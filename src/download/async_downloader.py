"""
Asynchronous S3 Downloader - High-performance aioboto3 downloader

This module provides async S3 downloads using aioboto3 for parallel downloads
with connection pooling and exponential backoff retry.

Based on: pipeline_design/mac-optimized-pipeline.md
"""

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError
import aiofiles
import gzip
import asyncio
from io import BytesIO
from pathlib import Path
from typing import List, Dict, Any, Optional
import time
import logging

from ..core.exceptions import S3DownloadError

logger = logging.getLogger(__name__)


class AsyncS3Downloader:
    """
    High-performance async S3 downloader

    Features:
    - Parallel downloads via asyncio
    - Connection pooling (50 connections)
    - Exponential backoff retry
    - Progress tracking
    - 3-5x faster than sync downloader
    """

    def __init__(
        self,
        credentials: Dict[str, str],
        endpoint_url: str = 'https://files.polygon.io',
        max_retries: int = 5,
        timeout: int = 60,
        max_pool_connections: int = 50,
        max_concurrent: int = 8
    ):
        """
        Initialize async S3 downloader

        Args:
            credentials: Dict with 'access_key_id' and 'secret_access_key'
            endpoint_url: S3 endpoint URL
            max_retries: Maximum retry attempts
            timeout: Request timeout in seconds
            max_pool_connections: Max connections in pool
            max_concurrent: Max concurrent downloads
        """
        self.credentials = credentials
        self.endpoint_url = endpoint_url
        self.max_retries = max_retries
        self.timeout = timeout
        self.max_concurrent = max_concurrent

        # Configure aioboto3 client
        self.config = Config(
            max_pool_connections=max_pool_connections,
            retries={
                'max_attempts': max_retries,
                'mode': 'adaptive'
            },
            connect_timeout=timeout,
            read_timeout=timeout,
            tcp_keepalive=True,
        )

        # Statistics
        self.download_count = 0
        self.retry_count = 0
        self.error_count = 0

        # Semaphore for limiting concurrency
        self.semaphore = asyncio.Semaphore(max_concurrent)

        logger.info(
            f"AsyncS3Downloader initialized "
            f"(endpoint: {endpoint_url}, max_concurrent: {max_concurrent})"
        )

    async def download_one(
        self,
        bucket: str,
        key: str,
        decompress: bool = True
    ) -> BytesIO:
        """
        Download single file from S3 (async)

        Args:
            bucket: S3 bucket name
            key: S3 object key
            decompress: Whether to decompress gzip files

        Returns:
            BytesIO object with file contents

        Raises:
            S3DownloadError: If download fails after retries
        """
        async with self.semaphore:  # Limit concurrency
            retry_delay = 1  # Initial delay in seconds

            for attempt in range(1, self.max_retries + 1):
                try:
                    logger.debug(f"Downloading s3://{bucket}/{key} (attempt {attempt})")

                    # Create session and client
                    session = aioboto3.Session(
                        aws_access_key_id=self.credentials['access_key_id'],
                        aws_secret_access_key=self.credentials['secret_access_key'],
                    )

                    async with session.client(
                        's3',
                        endpoint_url=self.endpoint_url,
                        config=self.config
                    ) as s3:
                        # Download from S3
                        response = await s3.get_object(Bucket=bucket, Key=key)

                        async with response['Body'] as stream:
                            content = await stream.read()

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
                            await asyncio.sleep(retry_delay)
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

    async def download_batch(
        self,
        bucket: str,
        keys: List[str],
        decompress: bool = True
    ) -> List[Optional[BytesIO]]:
        """
        Download multiple files in parallel (async)

        Args:
            bucket: S3 bucket name
            keys: List of S3 object keys
            decompress: Whether to decompress gzip files

        Returns:
            List of BytesIO objects (None for failed downloads)

        Example:
            >>> downloader = AsyncS3Downloader(credentials)
            >>> keys = ['file1.csv.gz', 'file2.csv.gz']
            >>> results = await downloader.download_batch('bucket', keys)
        """
        logger.info(f"Downloading {len(keys)} files in parallel...")
        start_time = time.time()

        # Create download tasks
        tasks = [
            self.download_one(bucket, key, decompress)
            for key in keys
        ]

        # Execute in parallel with exception handling
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to None
        outputs = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to download {keys[i]}: {result}")
                outputs.append(None)
            else:
                outputs.append(result)

        elapsed = time.time() - start_time
        success_count = sum(1 for r in outputs if r is not None)

        logger.info(
            f"Downloaded {success_count}/{len(keys)} files "
            f"in {elapsed:.1f}s ({len(keys)/elapsed:.1f} files/sec)"
        )

        return outputs

    async def download_to_file(
        self,
        bucket: str,
        key: str,
        local_path: Path,
        decompress: bool = True
    ):
        """
        Download file from S3 and save to disk (async)

        Args:
            bucket: S3 bucket name
            key: S3 object key
            local_path: Local file path to save
            decompress: Whether to decompress gzip files
        """
        content = await self.download_one(bucket, key, decompress=decompress)

        # Create parent directory
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to file (async)
        async with aiofiles.open(local_path, 'wb') as f:
            await f.write(content.getvalue())

        logger.info(f"Saved to {local_path}")

    async def list_objects(
        self,
        bucket: str,
        prefix: str,
        max_keys: int = 1000
    ) -> List[str]:
        """
        List objects in S3 bucket with prefix (async)

        Args:
            bucket: S3 bucket name
            prefix: Key prefix to filter
            max_keys: Maximum keys to return

        Returns:
            List of object keys
        """
        try:
            session = aioboto3.Session(
                aws_access_key_id=self.credentials['access_key_id'],
                aws_secret_access_key=self.credentials['secret_access_key'],
            )

            async with session.client(
                's3',
                endpoint_url=self.endpoint_url,
                config=self.config
            ) as s3:
                response = await s3.list_objects_v2(
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

    def get_statistics(self) -> Dict[str, Any]:
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


async def main():
    """Command-line interface for async downloader"""
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
        downloader = AsyncS3Downloader(
            credentials={
                'access_key_id': s3_creds['access_key_id'],
                'secret_access_key': s3_creds['secret_access_key'],
            },
            endpoint_url=s3_creds.get('endpoint_url', 'https://files.polygon.io'),
            max_concurrent=8
        )

        print("✅ AsyncS3Downloader initialized")
        print(f"   Endpoint: {downloader.endpoint_url}")
        print(f"   Max concurrent: {downloader.max_concurrent}")
        print(f"   Max retries: {downloader.max_retries}")

        # Test: List objects
        print("\n📋 Testing list_objects...")
        bucket = s3_creds.get('bucket', 'flatfiles')
        prefix = 'us_stocks_sip/day_aggs_v1/2025/09/'

        keys = await downloader.list_objects(bucket, prefix, max_keys=5)
        print(f"   Found {len(keys)} files:")
        for key in keys[:5]:
            print(f"     - {key}")

        # Test: Batch download (if files exist)
        if keys:
            print(f"\n⬇️  Testing batch download ({len(keys[:2])} files)...")
            results = await downloader.download_batch(bucket, keys[:2])
            success = sum(1 for r in results if r is not None)
            print(f"   Downloaded: {success}/{len(keys[:2])} files")

        # Statistics
        stats = downloader.get_statistics()
        print(f"\n📊 Statistics:")
        print(f"   Downloads: {stats['download_count']}")
        print(f"   Retries: {stats['retry_count']}")
        print(f"   Errors: {stats['error_count']}")
        print(f"   Success rate: {stats['success_rate']:.1%}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
