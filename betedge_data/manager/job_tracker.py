"""
SQLite-backed job tracking system for background data processing tasks.

This module provides persistent job tracking using SQLite database for
monitoring the progress of long-running data processing operations.
"""

import logging
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional
from uuid import UUID

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    """Enumeration of possible job statuses."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class JobInfo:
    """Information about a background job."""

    job_id: UUID
    status: JobStatus
    total_items: int
    completed_items: int
    error_message: Optional[str]
    created_at: datetime
    updated_at: datetime
    idem_key: Optional[str] = None
    result_uri: Optional[str] = None
    params_json: Optional[str] = None

    @property
    def progress_percentage(self) -> float:
        """Calculate progress as percentage (0.0 to 100.0)."""
        if self.total_items <= 0:
            return 100.0 if self.status == JobStatus.COMPLETED else 0.0
        return (self.completed_items / self.total_items) * 100.0

    @property
    def is_finished(self) -> bool:
        """Check if job is in a terminal state."""
        return self.status in (JobStatus.COMPLETED, JobStatus.FAILED)

    def to_dict(self) -> dict:
        """Convert JobInfo to dictionary for JSON serialization."""
        return {
            "job_id": str(self.job_id),
            "status": self.status.value,
            "total_items": self.total_items,
            "completed_items": self.completed_items,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "progress_percentage": self.progress_percentage,
            "idem_key": self.idem_key,
            "result_uri": self.result_uri,
            "params_json": self.params_json,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "JobInfo":
        """Create JobInfo from dictionary (for deserialization)."""
        return cls(
            job_id=UUID(data["job_id"]),
            status=JobStatus(data["status"]),
            total_items=data["total_items"],
            completed_items=data["completed_items"],
            error_message=data.get("error_message"),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            idem_key=data.get("idem_key"),
            result_uri=data.get("result_uri"),
            params_json=data.get("params_json"),
        )

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "JobInfo":
        """Create JobInfo from SQLite row."""
        return cls(
            job_id=UUID(row["id"]),
            status=JobStatus(row["status"]),
            total_items=row["total_items"],
            completed_items=row["completed_items"],
            error_message=row["error_message"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            idem_key=row["idem_key"],
            result_uri=row["result_uri"],
            params_json=row["params_json"],
        )


class JobTracker:
    """
    SQLite-backed job tracking with thread-safe operations.

    Provides persistent job management for background data processing tasks
    with support for idempotent operations and result URIs.
    """

    def __init__(self, db_path: str = "jobs.db"):
        """
        Initialize SQLite job tracker.

        Args:
            db_path: Path to SQLite database file.
            persistence_file: Ignored for compatibility with original JobTracker.
        """
        self.db_path = Path(db_path)
        self._lock = threading.RLock()
        self._init_database()

        logger.info(f"Initialized SQLite job tracker with database: {self.db_path}")

    def _init_database(self) -> None:
        """Initialize database with required tables and configuration."""
        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with self._get_connection() as conn:
            # Configure SQLite for better performance and WAL mode
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")

            # Create jobs table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    total_items INTEGER NOT NULL,
                    completed_items INTEGER NOT NULL,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    idem_key TEXT UNIQUE,
                    result_uri TEXT,
                    params_json TEXT
                )
            """)

            # Create indexes for performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_updated_at ON jobs(updated_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_idem_key ON jobs(idem_key)")

            conn.commit()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a new database connection with row factory."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def create_job(self, job_id: UUID, total_items: int) -> JobInfo:
        """
        Create a new job entry.

        Args:
            job_id: Unique identifier for the job
            total_items: Total number of items to process

        Returns:
            JobInfo object for the created job

        Raises:
            ValueError: If job_id already exists or total_items is invalid
        """
        if total_items < 0:
            raise ValueError(f"total_items must be non-negative, got {total_items}")

        now = datetime.now(timezone.utc)
        job_info = JobInfo(
            job_id=job_id,
            status=JobStatus.PENDING,
            total_items=total_items,
            completed_items=0,
            error_message=None,
            created_at=now,
            updated_at=now,
        )

        with self._lock:
            with self._get_connection() as conn:
                try:
                    conn.execute(
                        """
                        INSERT INTO jobs (
                            id, status, total_items, completed_items, error_message,
                            created_at, updated_at, idem_key, result_uri, params_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            str(job_id),
                            job_info.status.value,
                            total_items,
                            0,
                            None,
                            now.isoformat(),
                            now.isoformat(),
                            None,
                            None,
                            None,
                        ),
                    )
                    conn.commit()
                except sqlite3.IntegrityError:
                    raise ValueError(f"Job {job_id} already exists")

        logger.info(f"Created job {job_id} with {total_items} items")
        return job_info

    def get_job_by_idem(self, idem_key: str) -> Optional[JobInfo]:
        """
        Get job by idempotency key.

        Args:
            idem_key: Idempotency key to search for

        Returns:
            JobInfo object if found, None otherwise
        """
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute("SELECT * FROM jobs WHERE idem_key = ?", (idem_key,))
                row = cursor.fetchone()
                return JobInfo.from_row(row) if row else None

    def update_progress(self, job_id: UUID, completed_items: int) -> None:
        """
        Update job progress.

        Args:
            job_id: Job identifier
            completed_items: Number of completed items

        Raises:
            ValueError: If job doesn't exist or completed_items is invalid
        """
        if completed_items < 0:
            raise ValueError(f"completed_items must be non-negative, got {completed_items}")

        with self._lock:
            with self._get_connection() as conn:
                # Get current job info
                cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (str(job_id),))
                row = cursor.fetchone()
                if not row:
                    raise ValueError(f"Job {job_id} not found")

                job_info = JobInfo.from_row(row)

                if completed_items > job_info.total_items:
                    logger.warning(
                        f"Job {job_id}: completed_items ({completed_items}) exceeds "
                        f"total_items ({job_info.total_items})"
                    )

                # Update progress and auto-complete if needed
                new_status = job_info.status
                if completed_items >= job_info.total_items and job_info.status == JobStatus.RUNNING:
                    new_status = JobStatus.COMPLETED
                    logger.info(f"Job {job_id} auto-completed: {completed_items}/{job_info.total_items} items")

                cursor = conn.execute(
                    """
                    UPDATE jobs 
                    SET completed_items = ?, status = ?, updated_at = ?
                    WHERE id = ?
                """,
                    (completed_items, new_status.value, datetime.now(timezone.utc).isoformat(), str(job_id)),
                )

                conn.commit()

    def update_status(self, job_id: UUID, status: JobStatus) -> None:
        """
        Update job status.

        Args:
            job_id: Job identifier
            status: New job status

        Raises:
            ValueError: If job doesn't exist or status transition is invalid
        """
        with self._lock:
            with self._get_connection() as conn:
                # Get current job info
                cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (str(job_id),))
                row = cursor.fetchone()
                if not row:
                    raise ValueError(f"Job {job_id} not found")

                job_info = JobInfo.from_row(row)
                old_status = job_info.status

                # Validate status transitions
                if old_status in (JobStatus.COMPLETED, JobStatus.FAILED) and status != old_status:
                    raise ValueError(f"Cannot change status from {old_status} to {status} - job is finished")

                cursor = conn.execute(
                    """
                    UPDATE jobs 
                    SET status = ?, updated_at = ?
                    WHERE id = ?
                """,
                    (status.value, datetime.now(timezone.utc).isoformat(), str(job_id)),
                )

                conn.commit()

                if old_status != status:
                    logger.info(f"Job {job_id} status changed: {old_status} → {status}")

    def mark_completed(self, job_id: UUID) -> None:
        """
        Mark job as completed.

        Args:
            job_id: Job identifier
        """
        self.update_status(job_id, JobStatus.COMPLETED)

    def mark_failed(self, job_id: UUID, error_message: str) -> None:
        """
        Mark job as failed with error message.

        Args:
            job_id: Job identifier
            error_message: Description of the error that caused failure
        """
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    """
                    UPDATE jobs 
                    SET status = ?, error_message = ?, updated_at = ?
                    WHERE id = ?
                """,
                    (JobStatus.FAILED.value, error_message, datetime.now(timezone.utc).isoformat(), str(job_id)),
                )

                if cursor.rowcount == 0:
                    raise ValueError(f"Job {job_id} not found")

                conn.commit()

        logger.error(f"Job {job_id} failed: {error_message}")

    def get_job(self, job_id: UUID) -> Optional[JobInfo]:
        """
        Get job information.

        Args:
            job_id: Job identifier

        Returns:
            JobInfo object if job exists, None otherwise
        """
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (str(job_id),))
                row = cursor.fetchone()
                return JobInfo.from_row(row) if row else None