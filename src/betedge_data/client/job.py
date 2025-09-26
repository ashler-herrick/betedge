"""
SQLite-backed job tracking system for background data processing tasks.

This module provides persistent job tracking using SQLite database for
monitoring the progress of long-running data processing operations.
"""

import logging
from logging.handlers import RotatingFileHandler
import sqlite3
import threading
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict
from uuid import UUID

logger = logging.getLogger(__name__)


def setup_job_logger(job_id: UUID, log_dir: str = "logs/jobs") -> logging.Logger:
    """
    Set up a dedicated logger for a specific job.

    Args:
        job_id: Unique identifier for the job
        log_dir: Directory to store job log files

    Returns:
        Logger instance configured for the specific job
    """
    # Create log directory if it doesn't exist
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # Create logger with job-specific name
    job_logger_name = f"job.{job_id}"
    job_logger = logging.getLogger(job_logger_name)

    # Avoid adding handlers multiple times
    if job_logger.handlers:
        return job_logger

    job_logger.setLevel(logging.INFO)

    # Create file handler with rotation
    log_file = log_path / f"job_{job_id}.log"
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
    )

    # Create console handler for immediate feedback
    console_handler = logging.StreamHandler()

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to logger
    job_logger.addHandler(file_handler)
    job_logger.addHandler(console_handler)

    # Prevent propagation to root logger to avoid duplicate messages
    job_logger.propagate = False

    return job_logger


class JobStatus(str, Enum):
    """Enumeration of possible job statuses."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class JobInfo:
    """Information about a job."""

    job_id: UUID
    status: JobStatus
    total_items: int
    completed_items: int
    error_message: Optional[str]
    created_at: datetime
    updated_at: datetime

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


def create_job(job_id: UUID, total_items: int) -> JobInfo:
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

    return job_info
