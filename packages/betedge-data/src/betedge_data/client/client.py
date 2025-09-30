import requests
import threading
import logging

from io import BytesIO
from queue import Queue, Empty
from typing import Optional
from enum import Enum


import polars as pl
import pyarrow.parquet as pq
import pyarrow as pa
from minio import Minio
from minio.error import S3Error

from betedge_data.client.requests import (
    OptionRequest,
    StockRequest,
    EarningsRequest,
)
from betedge_data.client.config import get_settings
from betedge_data.http_client import HTTPClient
from betedge_data.exceptions import NoDataAvailableError
from betedge_data.job import HTTPJob, FileWriteJob, ReturnType, Schema
from betedge_data.processing.dispatch import process_http_result

Request = OptionRequest | StockRequest | EarningsRequest


logger = logging.getLogger(__name__)


class LogLevel(Enum):
    DEBUG = "debug"
    INFO = "info"
    WARN = "warn"
    ERROR = "error"


def _set_log_level(lvl: str | LogLevel) -> None:
    if isinstance(lvl, str):
        if lvl == "debug":
            lvl = LogLevel.DEBUG
        elif lvl == "info":
            lvl = LogLevel.INFO
        elif lvl == "warn":
            lvl = LogLevel.WARN
        elif lvl == "error":
            lvl = LogLevel.ERROR
        else:
            raise RuntimeError(
                f"Got unknown log level string '{lvl}. Valid options are 'debug', 'info', 'warn', 'error'."
            )

    match lvl:
        case LogLevel.DEBUG:
            logging.getLogger("betedge_data").setLevel(logging.DEBUG)
        case LogLevel.INFO:
            logging.getLogger("betedge_data").setLevel(logging.INFO)
        case LogLevel.WARN:
            logging.getLogger("betedge_data").setLevel(logging.WARNING)
        case LogLevel.ERROR:
            logging.getLogger("betedge_data").setLevel(logging.ERROR)
        case _:
            logging.getLogger("betedge_data").setLevel(logging.WARNING)


class BetEdgeClient:
    _instance = None

    def __init__(
        self,
        num_threads: Optional[int] = None,
        log_level: str | LogLevel = LogLevel.WARN,
    ) -> None:
        """Initialize the client, creates a DataProcessingService and checks ThetaTerminal connection."""
        _set_log_level(log_level)
        self.settings = get_settings()
        self.minio_config = self.settings.minio
        self.general_config = self.settings.general

        self._running = False
        self._workers_started = False
        self._shutdown_lock = threading.Lock()
        self._worker_exception: Optional[Exception] = None
        self._exception_lock = threading.Lock()

        if not num_threads:
            self.max_workers = self.general_config.max_workers
        else:
            self.max_workers = num_threads

        self.minio_client = Minio(
            endpoint=self.minio_config.endpoint,
            access_key=self.minio_config.access_key,
            secret_key=self.minio_config.secret_key,
            secure=self.minio_config.secure,
            region="us-east-1",
        )
        self._ensure_bucket_exists()

        self.http_client = HTTPClient(
            max_connections=self.max_workers, max_keepalive_connections=self.max_workers
        )
        self._ensure_theta_running()

        # Queues for processing Async
        self.http_job_queue: Queue[HTTPJob] = Queue()
        self.http_result_queue: Queue[HTTPJob] = Queue()
        self.file_write_queue: Queue[FileWriteJob] = Queue()

    # Singleton to prevent too many requests being sent.
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def _start(self):
        self.running = True
        logger.info(
            f"Starting BetEdge client with {self.max_workers} worker threads per pool"
        )

        # Start HTTP worker threads
        logger.info(f"Starting {self.max_workers} HTTP worker threads")
        for i in range(self.max_workers):
            thread = threading.Thread(
                target=self._http_worker, daemon=True, name=f"http-worker-{i}"
            )
            thread.start()
            logger.debug(
                f"Started HTTP worker thread: {thread.name} (ID: {thread.ident})"
            )

        # Start response processor threads
        workers = self.max_workers // 2
        logger.info(f"Starting {workers} response processing threads")
        for i in range(workers):
            thread = threading.Thread(
                target=self._response_processor,
                daemon=True,
                name=f"response-processor-{i}",
            )
            thread.start()
            logger.debug(
                f"Started response processor thread: {thread.name} (ID: {thread.ident})"
            )

        # Start file writer thread
        logger.info("Starting file writer thread.")
        thread = threading.Thread(
            target=self._file_writer, daemon=True, name="file-writer"
        )
        thread.start()
        logger.debug(f"Started file writer thread: {thread.name} (ID: {thread.ident})")

    def _shutdown(self):
        with self._shutdown_lock:
            if not self._running:
                return

            logger.info("Received shutdown command.")
            self._running = False

            # Clear all the queues
            while not self.http_job_queue.empty():
                try:
                    self.http_job_queue.get_nowait()
                    self.http_job_queue.task_done()
                except Empty:
                    break

            while not self.http_result_queue.empty():
                try:
                    self.http_result_queue.get_nowait()
                    self.http_result_queue.task_done()
                except Empty:
                    break

            while not self.file_write_queue.empty():
                try:
                    self.file_write_queue.get_nowait()
                    self.file_write_queue.task_done()
                except Empty:
                    break

    def _set_worker_exception(self, exc: Exception) -> None:
        with self._exception_lock:
            if self._worker_exception is None:
                self._worker_exception = exc
                logger.error(f"Caught worker exception: {exc}")
                self._shutdown()

    def _check_worker_exception(self) -> None:
        with self._exception_lock:
            if self._worker_exception is not None:
                exc = self._worker_exception
                self._worker_exception = None
                raise exc

    def _ensure_bucket_exists(self):
        """Ensure the configured bucket exists, create if not."""
        try:
            if not self.minio_client.bucket_exists(self.minio_config.bucket):
                logger.info(f"Creating bucket: {self.minio_config.bucket}")
                self.minio_client.make_bucket(
                    self.minio_config.bucket, location="us-east-1"
                )
                logger.info(f"Bucket {self.minio_config.bucket} created successfully")
            else:
                logger.debug(f"Bucket {self.minio_config.bucket} already exists")
        except S3Error as e:
            logger.error(f"Failed to ensure bucket exists: {e}")
            raise RuntimeError(f"MinIO bucket setup failed: {e}") from e

    def _http_worker(self):
        thread_name = threading.current_thread().name
        logger.debug(f"HTTP worker {thread_name} started and waiting for jobs")

        while self.running:
            try:
                job = self.http_job_queue.get(timeout=1)
                logger.debug(
                    f"HTTP worker {thread_name} picked up job for URL: {job.url}"
                )

                try:
                    job = self.http_client.fetch(
                        job
                    )  # This could raise a NoDataAvailableError
                    if job:
                        self.http_result_queue.put(job)
                    self.http_job_queue.task_done()

                except NoDataAvailableError:
                    logger.info("Got no data available error for, skipping.")
                    self.http_job_queue.task_done()

                except Exception as e:
                    logger.error(
                        f"HTTP worker {thread_name} failed to process job: {e}"
                    )
                    self.http_job_queue.task_done()  # Still mark as done to prevent hanging
                    self._set_worker_exception(e)

            except Empty:  # Exception for empty Queue
                continue  # Just continue polling

    def _response_processor(self):
        thread_name = threading.current_thread().name
        logger.debug(
            f"Response processor {thread_name} started and waiting for HTTP results"
        )

        while self.running:
            try:
                http_result = self.http_result_queue.get(timeout=1)
                logger.debug(
                    f"Response processor {thread_name} processing result for URL: {http_result.url}"
                )

                try:
                    file_write_job = process_http_result(http_result)
                    logger.debug(
                        f"Response processor {thread_name} converted HTTP result to file write job: {file_write_job.object_key}"
                    )

                    if file_write_job.completed:
                        self.file_write_queue.put(file_write_job)
                        logger.debug(
                            f"Response processor {thread_name} queued completed file write job: {file_write_job.object_key}"
                        )

                    self.http_result_queue.task_done()
                    logger.debug(
                        f"Response processor {thread_name} marked HTTP result task as done"
                    )

                except NoDataAvailableError:
                    logger.info(
                        f"Got no data available error for {http_result.url}, skipping."
                    )
                    self.http_result_queue.task_done()
                except Exception as e:
                    logger.error(
                        f"Response processor {thread_name} failed to process result for URL {http_result.url}: {e}"
                    )
                    self.http_result_queue.task_done()  # Still mark as done to prevent hanging
                    self._set_worker_exception(e)

            except Empty:
                continue

    def _file_writer(self):
        thread_name = threading.current_thread().name
        logger.debug(
            f"File writer {thread_name} started and waiting for file write jobs"
        )

        while self.running:
            try:
                file_write_job = self.file_write_queue.get(timeout=1)
                logger.debug(
                    f"File writer {thread_name} processing file write job: {file_write_job.object_key}"
                )

                try:
                    if not file_write_job.completed:
                        raise RuntimeError("Incomplete FileWriteJob found in Queue.")

                    logger.debug(
                        f"File writer {thread_name} concatenating {len(file_write_job.tables)} tables for {file_write_job.object_key}"
                    )
                    table = pa.concat_tables(file_write_job.tables)

                    buffer = BytesIO()
                    pq.write_table(table, buffer)
                    buffer.seek(0)

                    size = len(buffer.getvalue())
                    logger.info(
                        f"File writer {thread_name} writing {size} bytes to MinIO object: {file_write_job.object_key}"
                    )

                    self.minio_client.put_object(
                        bucket_name=self.minio_config.bucket,
                        object_name=file_write_job.object_key,
                        data=buffer,
                        length=size,
                        content_type="application/octet-stream",
                    )

                    logger.info(
                        f"File writer {thread_name} successfully uploaded object to MinIO: {file_write_job.object_key}"
                    )
                    self.file_write_queue.task_done()

                except Exception as e:
                    logger.error(
                        f"File writer {thread_name} failed to write file {file_write_job.object_key}: {e}"
                    )
                    self.file_write_queue.task_done()  # Still mark as done to prevent hanging
                    self._set_worker_exception(e)

            except Empty:
                continue

    def request_data(self, request: Request) -> None:
        logger.info(
            f"Processing data request for {type(request).__name__} (ID: {request.id})"
        )
        self._start()
        key_map = request.get_key_map()
        headers = request.headers
        schema = Schema.EARNINGS
        return_type = ReturnType.JSON

        if isinstance(request, OptionRequest):
            return_type = ReturnType.CSV
            if request.endpoint == "eod":
                schema = Schema.OPTION_EOD
            else:
                schema = Schema.OPTION_QUOTE

        elif isinstance(request, StockRequest):
            if request.endpoint == "eod":
                schema = Schema.STOCK_EOD
            else:
                schema = Schema.STOCK_QUOTE

            return_type = ReturnType.CSV

        total_jobs = 0
        total_files = len(key_map)
        files_skipped = 0

        for object_key, url_list in key_map.items():
            if not request.force_refresh and self._file_exists(object_key):
                files_skipped += 1
                logger.info(f"Skipping existing file: {object_key}")
                continue
            else:
                file_write_job = FileWriteJob(object_key, len(url_list))
                logger.info(
                    f"Creating {len(url_list)} HTTP jobs for file: {object_key}"
                )
                for url in url_list:
                    job = HTTPJob(
                        url=url,
                        schema=schema,
                        return_type=return_type,
                        file_write_job=file_write_job,
                        headers=headers,
                    )
                    self.http_job_queue.put(job)
                    total_jobs += 1

        logger.info(
            f"Queued {total_jobs} HTTP jobs for {total_files - files_skipped} files ({files_skipped} files skipped)"
        )

        self.http_job_queue.join()
        logger.info("All HTTP jobs completed")
        self._check_worker_exception()

        self.http_result_queue.join()
        logger.info("All response processing completed")
        self._check_worker_exception()

        self.file_write_queue.join()
        self._check_worker_exception()
        logger.info(
            f"Request processing completed for {type(request).__name__} (ID: {request.id})"
        )

    def retrieve_data(self, request: Request) -> pl.DataFrame:
        logger.info(
            f"Processing retrieval request for {type(request).__name__} (ID: {request.id})"
        )

        key_map = request.get_key_map()
        uris = [
            f"s3://{self.minio_config.bucket}/{key}"
            for key in key_map.keys()
            if self._file_exists(key)
        ]
        storage_options = self.minio_config.get_minio_storage_options()
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
