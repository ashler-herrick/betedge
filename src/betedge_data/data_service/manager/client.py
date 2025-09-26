import uuid
import os
import polars as pl
from dotenv import load_dotenv

from betedge_data.data_service.manager.service import DataProcessingService
from betedge_data.data_service.manager.external_models import ExternalBaseRequest
from betedge_data.data_service.manager.job_tracker import JobInfo


class BetEdgeClient:
    def __init__(self) -> None:
        """Initialize the client, creates a DataProcessingService to use."""
        self.service = DataProcessingService()

    async def request_data(self, request: ExternalBaseRequest) -> JobInfo:
        """
        A request to populate the data lake with files.

        Args:
            request(ExternalBaseRequest): ExternalBaseRequest object containing the information needed to populate the data lake.

        Returns:
            A JobInfo object
        """
        request_id = uuid.uuid4()
        job_info = await self.service.process_request(request=request, request_id=request_id)
        return job_info

    def retrieve_data(self, request: ExternalBaseRequest) -> pl.LazyFrame:
        """
        A request to retrieve data from the data lake and return it as a dataframe.

        Args:
            request(ExternalBaseRequest): ExternalBaseRequest object with the parameters needed to retrieve data from the lake.

        Returns:
            A polars DataFrame with the data.
        """
        subrequests = request.get_subrequests()
        object_keys = [sub.generate_object_key() for sub in subrequests]
        storage_options = self._get_minio_storage_options()
        lazy_dfs = [pl.scan_parquet(key, storage_options=storage_options) for key in object_keys]
        return pl.concat(lazy_dfs)

    def get_job_status(self, job_info: JobInfo) -> JobInfo:
        """
        Get a new JobInfo object from a current one.

        Args:
            job_info(JobInfo): Job info returned from request_data call.

        Returns:
            A new JobInfo object.
        """

    def _get_minio_storage_options(self) -> dict:
        """Convert MinIO env vars to Polars-compatible storage options"""
        endpoint = os.getenv("MINIO_ENDPOINT")
        secure = os.getenv("MINIO_SECURE", "true").lower() == "true"

        # Build the endpoint URL
        protocol = "https" if secure else "http"
        endpoint_url = f"{protocol}://{endpoint}"

        return {
            "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY"),
            "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY"),
            "aws_endpoint_url": endpoint_url,
            "aws_region": os.getenv("MINIO_REGION", "us-east-1"),
            "aws_allow_http": str(not secure).lower(),
        }
