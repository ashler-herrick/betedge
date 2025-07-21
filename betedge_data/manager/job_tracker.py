"""
Job tracking system for background data processing tasks.

This module provides in-memory job tracking with optional persistence for
monitoring the progress of long-running data processing operations.
"""

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, Optional
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
            "progress_percentage": self.progress_percentage
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
            updated_at=datetime.fromisoformat(data["updated_at"])
        )


class JobTracker:
    """
    In-memory job tracking with optional file persistence.
    
    Provides thread-safe job management for background data processing tasks.
    Optionally persists job state to disk for recovery across service restarts.
    """
    
    def __init__(self, persistence_file: Optional[str] = None):
        """
        Initialize job tracker.
        
        Args:
            persistence_file: Optional path to JSON file for job persistence.
                            If provided, jobs will be saved/loaded from this file.
        """
        self._jobs: Dict[UUID, JobInfo] = {}
        self._lock = threading.RLock()  # Reentrant lock for nested calls
        self.persistence_file = Path(persistence_file) if persistence_file else None
        
        # Load existing jobs from persistence file if it exists
        if self.persistence_file and self.persistence_file.exists():
            self._load_jobs()
            logger.info(f"Loaded {len(self._jobs)} jobs from {self.persistence_file}")
    
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
            updated_at=now
        )
        
        with self._lock:
            if job_id in self._jobs:
                raise ValueError(f"Job {job_id} already exists")
            
            self._jobs[job_id] = job_info
            self._persist_jobs()
            
        logger.info(f"Created job {job_id} with {total_items} items")
        return job_info
    
    def update_progress(self, job_id: UUID, completed_items: int) -> None:
        """
        Update job progress.
        
        Args:
            job_id: Job identifier
            completed_items: Number of completed items
            
        Raises:
            ValueError: If job doesn't exist or completed_items is invalid
        """
        with self._lock:
            job_info = self._get_job_unsafe(job_id)
            
            if completed_items < 0:
                raise ValueError(f"completed_items must be non-negative, got {completed_items}")
            
            if completed_items > job_info.total_items:
                logger.warning(
                    f"Job {job_id}: completed_items ({completed_items}) exceeds "
                    f"total_items ({job_info.total_items})"
                )
            
            job_info.completed_items = completed_items
            job_info.updated_at = datetime.now(timezone.utc)
            
            # Auto-complete job if all items are done
            if completed_items >= job_info.total_items and job_info.status == JobStatus.RUNNING:
                job_info.status = JobStatus.COMPLETED
                logger.info(f"Job {job_id} auto-completed: {completed_items}/{job_info.total_items} items")
            
            self._persist_jobs()
    
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
            job_info = self._get_job_unsafe(job_id)
            old_status = job_info.status
            
            # Validate status transitions
            if old_status in (JobStatus.COMPLETED, JobStatus.FAILED) and status != old_status:
                raise ValueError(f"Cannot change status from {old_status} to {status} - job is finished")
            
            job_info.status = status
            job_info.updated_at = datetime.now(timezone.utc)
            
            if old_status != status:
                logger.info(f"Job {job_id} status changed: {old_status} → {status}")
            
            self._persist_jobs()
    
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
            job_info = self._get_job_unsafe(job_id)
            job_info.status = JobStatus.FAILED
            job_info.error_message = error_message
            job_info.updated_at = datetime.now(timezone.utc)
            
            logger.error(f"Job {job_id} failed: {error_message}")
            self._persist_jobs()
    
    def get_job(self, job_id: UUID) -> Optional[JobInfo]:
        """
        Get job information.
        
        Args:
            job_id: Job identifier
            
        Returns:
            JobInfo object if job exists, None otherwise
        """
        with self._lock:
            return self._jobs.get(job_id)
    
    def list_jobs(self, status_filter: Optional[JobStatus] = None) -> Dict[UUID, JobInfo]:
        """
        List all jobs, optionally filtered by status.
        
        Args:
            status_filter: Optional status to filter by
            
        Returns:
            Dictionary of job_id -> JobInfo for matching jobs
        """
        with self._lock:
            if status_filter is None:
                return dict(self._jobs)
            
            return {
                job_id: job_info 
                for job_id, job_info in self._jobs.items()
                if job_info.status == status_filter
            }
    
    def cleanup_finished_jobs(self, max_age_hours: int = 24) -> int:
        """
        Remove finished jobs older than specified age.
        
        Args:
            max_age_hours: Maximum age in hours for finished jobs
            
        Returns:
            Number of jobs removed
        """
        cutoff_time = datetime.now(timezone.utc).replace(microsecond=0) - \
                      timedelta(hours=max_age_hours)
        
        with self._lock:
            to_remove = []
            for job_id, job_info in self._jobs.items():
                if job_info.is_finished and job_info.updated_at < cutoff_time:
                    to_remove.append(job_id)
            
            for job_id in to_remove:
                del self._jobs[job_id]
            
            if to_remove:
                self._persist_jobs()
                logger.info(f"Cleaned up {len(to_remove)} finished jobs older than {max_age_hours}h")
            
            return len(to_remove)
    
    def _get_job_unsafe(self, job_id: UUID) -> JobInfo:
        """Get job without locking (internal use only)."""
        job_info = self._jobs.get(job_id)
        if job_info is None:
            raise ValueError(f"Job {job_id} not found")
        return job_info
    
    def _persist_jobs(self) -> None:
        """Save jobs to persistence file if configured."""
        if not self.persistence_file:
            return
        
        try:
            # Convert jobs to serializable format
            jobs_data = {
                str(job_id): job_info.to_dict()
                for job_id, job_info in self._jobs.items()
            }
            
            # Ensure directory exists
            self.persistence_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to temporary file first, then rename for atomicity
            temp_file = self.persistence_file.with_suffix('.tmp')
            with temp_file.open('w') as f:
                json.dump(jobs_data, f, indent=2)
            
            temp_file.replace(self.persistence_file)
            
        except Exception as e:
            logger.error(f"Failed to persist jobs to {self.persistence_file}: {e}")
    
    def _load_jobs(self) -> None:
        """Load jobs from persistence file."""
        try:
            with self.persistence_file.open('r') as f:
                jobs_data = json.load(f)
            
            for job_id_str, job_dict in jobs_data.items():
                try:
                    job_id = UUID(job_id_str)
                    job_info = JobInfo.from_dict(job_dict)
                    self._jobs[job_id] = job_info
                except Exception as e:
                    logger.warning(f"Failed to load job {job_id_str}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to load jobs from {self.persistence_file}: {e}")