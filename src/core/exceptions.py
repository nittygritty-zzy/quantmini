"""
Custom Exceptions for the Data Pipeline

This module defines custom exceptions used throughout the pipeline.

Based on: pipeline_design/mac-optimized-pipeline.md
"""


class PipelineException(Exception):
    """Base exception for all pipeline errors"""
    pass


class ConfigurationError(PipelineException):
    """Configuration-related errors"""
    pass


class MemoryLimitExceeded(PipelineException):
    """Process memory exceeded configured limit"""
    pass


class S3DownloadError(PipelineException):
    """S3 download failed"""
    pass


class DataValidationError(PipelineException):
    """Data validation failed"""
    pass


class IngestionError(PipelineException):
    """Data ingestion error"""
    pass


class FeatureEngineeringError(PipelineException):
    """Feature computation error"""
    pass


class BinaryConversionError(PipelineException):
    """Binary format conversion error"""
    pass


class WatermarkError(PipelineException):
    """Watermark management error"""
    pass


class APIError(PipelineException):
    """Polygon API request error"""
    pass
