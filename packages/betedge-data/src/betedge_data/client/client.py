import uuid
import requests
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple, Optional

import polars as pl
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

from betedge_data.client.client_requests import BaseClientRequest
from betedge_data.client.client_requests import Request
from betedge_data.client.job import JobInfo, JobStatus, create_job
from betedge_data.client.minio import MinIOConfig

load_dotenv()

logging.basicConfig(
    format="%(asctime)s | Thread-%(thread)d (%(threadName)s) | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


class BetEdgeClient:
    def __init__(
        self, max_workers: int = int(os.getenv("HTTP_CONCURRENCY") or 4)
    ) -> None:
        """Initialize the client, creates a DataProcessingService and checks ThetaTerminal connection."""
        self.minio_config = MinIOConfig()
        self.minio_client = Minio(
            endpoint=self.minio_config.endpoint,
            access_key=self.minio_config.access_key,
            secret_key=self.minio_config.secret_key,
            secure=self.minio_config.secure,
            region=self.minio_config.region,
        )
        self._ensure_theta_running()
        self.max_workers = max_workers

    def _ensure_bucket_exists(self):
        """Ensure the configured bucket exists, create if not."""
        try:
            if not self.minio_client.bucket_exists(self.minio_config.bucket):
                logger.info(f"Creating bucket: {self.minio_config.bucket}")
                self.minio_client.make_bucket(
                    self.minio_config.bucket, location=self.minio_config.region
                )
                logger.info(f"Bucket {self.minio_config.bucket} created successfully")
            else:
                logger.debug(f"Bucket {self.minio_config.bucket} already exists")
        except S3Error as e:
            logger.error(f"Failed to ensure bucket exists: {e}")
            raise RuntimeError(f"MinIO bucket setup failed: {e}") from e

    def request_data(self, request: BaseClientRequest, max_retries: int = 1) -> JobInfo:
        """
        A request to populate the data lake with files.

        Args:
            request(ExternalBaseRequest): ExternalBaseRequest object containing the information needed to populate the data lake.

        Returns:
            A JobInfo object
        """
        request_id = uuid.uuid4()
        logger.info(f"Processing request for {request}.")

        reqs = request.get_subrequests()
        total_items = len(reqs)
        job_info = create_job(request_id, total_items)
        completed = 0

        while i := 0 < max_retries and completed < total_items:
            errors = ""
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(self._process_subrequest, req, request)
                    for req in reqs
                ]

                for future in as_completed(futures):
                    result = future.result()  # A tuple containing a bool indicating success/failure and error messages

                    if result[0]:
                        completed += 1
                    else:
                        errors += str(result[1])

            job_info.completed_items = completed
            if job_info.completed_items != job_info.total_items:
                job_info.status = JobStatus.FAILED
                job_info.error_message = errors
            else:
                job_info.status = JobStatus.COMPLETED
            i += 1

        return job_info

    def _process_subrequest(
        self, subreq: Request, request: BaseClientRequest
    ) -> Tuple[bool, Optional[str]]:
        try:
            object_key = subreq.generate_object_key()

            if not request.force_refresh and self._file_exists(object_key):
                return (True, None)
            else:
                try:
                    self.minio_client.remove_object(
                        self.minio_config.bucket, object_key
                    )
                except S3Error:
                    pass

            client = request.get_client()

            byte_wrapper = client.get_data(subreq)

            size = len(byte_wrapper.getvalue())

            self.minio_client.put_object(
                bucket_name=self.minio_config.bucket,
                object_name=object_key,
                data=byte_wrapper,
                length=size,
                content_type="application/octet-stream",
            )
            logger.info(f"Successfully uploaded {object_key} to MinIO. Size: {size}")
            return (True, None)
        except Exception as e:
            return (False, str(e))

    def retrieve_data(self, request: BaseClientRequest) -> pl.DataFrame:
        """
        A request to retrieve data from the data lake and return it as a dataframe.

        Args:
            request(ExternalBaseRequest): ExternalBaseRequest object with the parameters needed to retrieve data from the lake.

        Returns:
            A polars DataFrame with the data.
        """
        # Get keys from subrequests
        subrequests = request.get_subrequests()
        object_keys = [sub.generate_object_key() for sub in subrequests]

        uris = [
            f"s3://{self.minio_config.bucket}/{key}"
            for key in object_keys
            if self._file_exists(key)
        ]

        # Create lazyframe from all the keys
        storage_options = self._get_minio_storage_options()
        dfs = [pl.read_parquet(uri, storage_options=storage_options) for uri in uris]

        return pl.concat(dfs)

    def _file_exists(self, object_key: str) -> bool:
        try:
            self.minio_client.stat_object(self.minio_config.bucket, object_key)
            return True
        except S3Error:
            return False

    def _ensure_theta_running(self) -> None:
        """Ensure ThetaTerminal is accessible."""
        try:
            requests.get(
                "http://127.0.0.1:25510/v2/list/dates/stock/quote?root=AAPL", timeout=5
            )
        except Exception:
            raise RuntimeError(
                "Cannot connect to ThetaTerminal. Please start it manually:\n"
                "java -jar ThetaTerminal.jar"
            )

    def _get_minio_storage_options(self) -> dict:
        """Convert MinIO config vars to Polars-compatible storage options"""
        endpoint = self.minio_config.endpoint
        secure = self.minio_config.secure

        # Build the endpoint URL
        protocol = "https" if secure else "http"
        endpoint_url = f"{protocol}://{endpoint}"

        return {
            "aws_access_key_id": self.minio_config.access_key,
            "aws_secret_access_key": self.minio_config.secret_key,
            "aws_endpoint_url": endpoint_url,
            "aws_region": "us-east-1",
            "aws_allow_http": str(not secure).lower(),
        }
