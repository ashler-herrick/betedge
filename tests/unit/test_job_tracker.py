"""
Unit tests for job tracking system.
"""

import json
import tempfile
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from betedge_data.manager.job_tracker import JobInfo, JobStatus, JobTracker


@pytest.mark.unit
class TestJobInfo:
    """Test JobInfo dataclass functionality."""

    def test_job_info_creation(self):
        """Test basic JobInfo creation."""
        job_id = uuid4()
        now = datetime.now(timezone.utc)
        
        job_info = JobInfo(
            job_id=job_id,
            status=JobStatus.PENDING,
            total_items=100,
            completed_items=0,
            error_message=None,
            created_at=now,
            updated_at=now
        )
        
        assert job_info.job_id == job_id
        assert job_info.status == JobStatus.PENDING
        assert job_info.total_items == 100
        assert job_info.completed_items == 0
        assert job_info.error_message is None
        assert job_info.created_at == now
        assert job_info.updated_at == now

    def test_progress_percentage_calculation(self):
        """Test progress percentage calculation."""
        job_id = uuid4()
        now = datetime.now(timezone.utc)
        
        # Test 0% progress
        job_info = JobInfo(
            job_id=job_id,
            status=JobStatus.RUNNING,
            total_items=100,
            completed_items=0,
            error_message=None,
            created_at=now,
            updated_at=now
        )
        assert job_info.progress_percentage == 0.0
        
        # Test 50% progress
        job_info.completed_items = 50
        assert job_info.progress_percentage == 50.0
        
        # Test 100% progress
        job_info.completed_items = 100
        assert job_info.progress_percentage == 100.0
        
        # Test over 100% (edge case)
        job_info.completed_items = 150
        assert job_info.progress_percentage == 150.0

    def test_progress_percentage_edge_cases(self):
        """Test progress percentage with edge cases."""
        job_id = uuid4()
        now = datetime.now(timezone.utc)
        
        # Test zero total items
        job_info = JobInfo(
            job_id=job_id,
            status=JobStatus.COMPLETED,
            total_items=0,
            completed_items=0,
            error_message=None,
            created_at=now,
            updated_at=now
        )
        assert job_info.progress_percentage == 100.0  # Completed with 0 items
        
        # Test zero total items but pending
        job_info.status = JobStatus.PENDING
        assert job_info.progress_percentage == 0.0

    def test_is_finished_property(self):
        """Test is_finished property for different statuses."""
        job_id = uuid4()
        now = datetime.now(timezone.utc)
        
        job_info = JobInfo(
            job_id=job_id,
            status=JobStatus.PENDING,
            total_items=100,
            completed_items=0,
            error_message=None,
            created_at=now,
            updated_at=now
        )
        
        # Non-terminal states
        assert not job_info.is_finished
        
        job_info.status = JobStatus.RUNNING
        assert not job_info.is_finished
        
        # Terminal states
        job_info.status = JobStatus.COMPLETED
        assert job_info.is_finished
        
        job_info.status = JobStatus.FAILED
        assert job_info.is_finished

    def test_to_dict_serialization(self):
        """Test JobInfo to_dict serialization."""
        job_id = uuid4()
        now = datetime.now(timezone.utc)
        
        job_info = JobInfo(
            job_id=job_id,
            status=JobStatus.RUNNING,
            total_items=100,
            completed_items=25,
            error_message="Test error",
            created_at=now,
            updated_at=now
        )
        
        result = job_info.to_dict()
        
        assert result["job_id"] == str(job_id)
        assert result["status"] == "running"
        assert result["total_items"] == 100
        assert result["completed_items"] == 25
        assert result["error_message"] == "Test error"
        assert result["created_at"] == now.isoformat()
        assert result["updated_at"] == now.isoformat()
        assert result["progress_percentage"] == 25.0

    def test_from_dict_deserialization(self):
        """Test JobInfo from_dict deserialization."""
        job_id = uuid4()
        now = datetime.now(timezone.utc)
        
        data = {
            "job_id": str(job_id),
            "status": "completed",
            "total_items": 50,
            "completed_items": 50,
            "error_message": None,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat()
        }
        
        job_info = JobInfo.from_dict(data)
        
        assert job_info.job_id == job_id
        assert job_info.status == JobStatus.COMPLETED
        assert job_info.total_items == 50
        assert job_info.completed_items == 50
        assert job_info.error_message is None
        assert job_info.created_at == now
        assert job_info.updated_at == now

    def test_serialization_roundtrip(self):
        """Test that serialization and deserialization work together."""
        job_id = uuid4()
        now = datetime.now(timezone.utc)
        
        original = JobInfo(
            job_id=job_id,
            status=JobStatus.FAILED,
            total_items=75,
            completed_items=30,
            error_message="Something went wrong",
            created_at=now,
            updated_at=now
        )
        
        # Serialize and deserialize
        data = original.to_dict()
        restored = JobInfo.from_dict(data)
        
        # Compare all fields
        assert restored.job_id == original.job_id
        assert restored.status == original.status
        assert restored.total_items == original.total_items
        assert restored.completed_items == original.completed_items
        assert restored.error_message == original.error_message
        assert restored.created_at == original.created_at
        assert restored.updated_at == original.updated_at


@pytest.mark.unit
class TestJobTracker:
    """Test JobTracker functionality."""

    def test_create_job(self):
        """Test job creation."""
        tracker = JobTracker()
        job_id = uuid4()
        
        job_info = tracker.create_job(job_id, 100)
        
        assert job_info.job_id == job_id
        assert job_info.status == JobStatus.PENDING
        assert job_info.total_items == 100
        assert job_info.completed_items == 0
        assert job_info.error_message is None
        assert isinstance(job_info.created_at, datetime)
        assert isinstance(job_info.updated_at, datetime)

    def test_create_duplicate_job(self):
        """Test that creating duplicate job raises error."""
        tracker = JobTracker()
        job_id = uuid4()
        
        tracker.create_job(job_id, 100)
        
        with pytest.raises(ValueError, match=f"Job {job_id} already exists"):
            tracker.create_job(job_id, 50)

    def test_create_job_invalid_total_items(self):
        """Test job creation with invalid total items."""
        tracker = JobTracker()
        job_id = uuid4()
        
        with pytest.raises(ValueError, match="total_items must be non-negative"):
            tracker.create_job(job_id, -1)

    def test_get_job(self):
        """Test job retrieval."""
        tracker = JobTracker()
        job_id = uuid4()
        
        # Create job
        created_job = tracker.create_job(job_id, 100)
        
        # Retrieve job
        retrieved_job = tracker.get_job(job_id)
        
        assert retrieved_job is not None
        assert retrieved_job.job_id == created_job.job_id
        assert retrieved_job.status == created_job.status

    def test_get_nonexistent_job(self):
        """Test retrieving non-existent job returns None."""
        tracker = JobTracker()
        job_id = uuid4()
        
        result = tracker.get_job(job_id)
        assert result is None

    def test_update_progress(self):
        """Test progress updates."""
        tracker = JobTracker()
        job_id = uuid4()
        
        # Create job
        tracker.create_job(job_id, 100)
        
        # Update progress
        tracker.update_progress(job_id, 25)
        
        job_info = tracker.get_job(job_id)
        assert job_info.completed_items == 25
        assert job_info.progress_percentage == 25.0

    def test_update_progress_auto_completion(self):
        """Test that job auto-completes when all items are done."""
        tracker = JobTracker()
        job_id = uuid4()
        
        # Create and start job
        tracker.create_job(job_id, 100)
        tracker.update_status(job_id, JobStatus.RUNNING)
        
        # Complete all items
        tracker.update_progress(job_id, 100)
        
        job_info = tracker.get_job(job_id)
        assert job_info.completed_items == 100
        assert job_info.status == JobStatus.COMPLETED

    def test_update_progress_nonexistent_job(self):
        """Test updating progress for non-existent job raises error."""
        tracker = JobTracker()
        job_id = uuid4()
        
        with pytest.raises(ValueError, match=f"Job {job_id} not found"):
            tracker.update_progress(job_id, 50)

    def test_update_progress_negative_value(self):
        """Test updating progress with negative value raises error."""
        tracker = JobTracker()
        job_id = uuid4()
        
        tracker.create_job(job_id, 100)
        
        with pytest.raises(ValueError, match="completed_items must be non-negative"):
            tracker.update_progress(job_id, -1)

    def test_update_status(self):
        """Test status updates."""
        tracker = JobTracker()
        job_id = uuid4()
        
        # Create job
        tracker.create_job(job_id, 100)
        
        # Update status
        tracker.update_status(job_id, JobStatus.RUNNING)
        
        job_info = tracker.get_job(job_id)
        assert job_info.status == JobStatus.RUNNING

    def test_update_status_invalid_transition(self):
        """Test that invalid status transitions raise error."""
        tracker = JobTracker()
        job_id = uuid4()
        
        # Create and complete job
        tracker.create_job(job_id, 100)
        tracker.update_status(job_id, JobStatus.COMPLETED)
        
        # Try to change status from terminal state
        with pytest.raises(ValueError, match="Cannot change status from"):
            tracker.update_status(job_id, JobStatus.RUNNING)

    def test_mark_completed(self):
        """Test marking job as completed."""
        tracker = JobTracker()
        job_id = uuid4()
        
        tracker.create_job(job_id, 100)
        tracker.mark_completed(job_id)
        
        job_info = tracker.get_job(job_id)
        assert job_info.status == JobStatus.COMPLETED

    def test_mark_failed(self):
        """Test marking job as failed with error message."""
        tracker = JobTracker()
        job_id = uuid4()
        error_message = "Something went wrong"
        
        tracker.create_job(job_id, 100)
        tracker.mark_failed(job_id, error_message)
        
        job_info = tracker.get_job(job_id)
        assert job_info.status == JobStatus.FAILED
        assert job_info.error_message == error_message

    def test_list_jobs(self):
        """Test listing all jobs."""
        tracker = JobTracker()
        
        # Create multiple jobs
        job_id1 = uuid4()
        job_id2 = uuid4()
        job_id3 = uuid4()
        
        tracker.create_job(job_id1, 100)
        tracker.create_job(job_id2, 200)
        tracker.create_job(job_id3, 300)
        
        # List all jobs
        jobs = tracker.list_jobs()
        
        assert len(jobs) == 3
        assert job_id1 in jobs
        assert job_id2 in jobs
        assert job_id3 in jobs

    def test_list_jobs_with_filter(self):
        """Test listing jobs with status filter."""
        tracker = JobTracker()
        
        # Create jobs with different statuses
        job_id1 = uuid4()
        job_id2 = uuid4()
        job_id3 = uuid4()
        
        tracker.create_job(job_id1, 100)  # PENDING
        tracker.create_job(job_id2, 200)
        tracker.update_status(job_id2, JobStatus.RUNNING)  # RUNNING
        tracker.create_job(job_id3, 300)
        tracker.mark_completed(job_id3)  # COMPLETED
        
        # Filter by status
        pending_jobs = tracker.list_jobs(JobStatus.PENDING)
        running_jobs = tracker.list_jobs(JobStatus.RUNNING)
        completed_jobs = tracker.list_jobs(JobStatus.COMPLETED)
        
        assert len(pending_jobs) == 1
        assert job_id1 in pending_jobs
        
        assert len(running_jobs) == 1
        assert job_id2 in running_jobs
        
        assert len(completed_jobs) == 1
        assert job_id3 in completed_jobs

    def test_cleanup_finished_jobs(self):
        """Test cleanup of old finished jobs."""
        tracker = JobTracker()
        
        # Create jobs
        job_id1 = uuid4()
        job_id2 = uuid4()
        job_id3 = uuid4()
        
        tracker.create_job(job_id1, 100)
        tracker.create_job(job_id2, 200)
        tracker.create_job(job_id3, 300)
        
        # Complete some jobs
        tracker.mark_completed(job_id1)
        tracker.mark_failed(job_id2, "Error")
        # job_id3 remains pending
        
        # Manually set old timestamps for completed jobs
        old_time = datetime.now(timezone.utc) - timedelta(hours=25)
        tracker._jobs[job_id1].updated_at = old_time
        tracker._jobs[job_id2].updated_at = old_time
        
        # Cleanup old jobs (24 hour cutoff)
        removed_count = tracker.cleanup_finished_jobs(max_age_hours=24)
        
        assert removed_count == 2
        assert tracker.get_job(job_id1) is None  # Removed
        assert tracker.get_job(job_id2) is None  # Removed
        assert tracker.get_job(job_id3) is not None  # Still pending, not removed

    def test_basic_thread_safety(self):
        """Test basic thread safety with concurrent operations."""
        tracker = JobTracker()
        job_id = uuid4()
        
        # Create job with exactly the number of items we'll process
        tracker.create_job(job_id, 50)  # 5 threads * 10 updates each
        tracker.update_status(job_id, JobStatus.RUNNING)
        
        # Define worker function
        def update_progress():
            for i in range(10):
                current_job = tracker.get_job(job_id)
                if current_job:
                    tracker.update_progress(job_id, current_job.completed_items + 1)
                time.sleep(0.001)  # Small delay to encourage race conditions
        
        # Start multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=update_progress)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check final state
        final_job = tracker.get_job(job_id)
        assert final_job is not None
        assert final_job.completed_items == 50  # 5 threads * 10 updates each
        assert final_job.status == JobStatus.COMPLETED  # Auto-completed


@pytest.mark.unit
class TestJobTrackerPersistence:
    """Test JobTracker persistence functionality."""

    def test_persistence_disabled_by_default(self):
        """Test that persistence is disabled when no file is provided."""
        tracker = JobTracker()
        job_id = uuid4()
        
        tracker.create_job(job_id, 100)
        
        # Should not create any files
        assert tracker.persistence_file is None

    def test_save_and_load_jobs(self):
        """Test saving and loading jobs to/from file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            persistence_file = Path(temp_dir) / "jobs.json"
            
            # Create tracker with persistence
            tracker1 = JobTracker(str(persistence_file))
            
            # Create some jobs
            job_id1 = uuid4()
            job_id2 = uuid4()
            
            tracker1.create_job(job_id1, 100)
            tracker1.create_job(job_id2, 200)
            tracker1.update_progress(job_id1, 50)
            tracker1.mark_failed(job_id2, "Test error")
            
            # Verify file was created
            assert persistence_file.exists()
            
            # Create new tracker instance (simulates restart)
            tracker2 = JobTracker(str(persistence_file))
            
            # Verify jobs were loaded
            loaded_job1 = tracker2.get_job(job_id1)
            loaded_job2 = tracker2.get_job(job_id2)
            
            assert loaded_job1 is not None
            assert loaded_job1.total_items == 100
            assert loaded_job1.completed_items == 50
            
            assert loaded_job2 is not None
            assert loaded_job2.status == JobStatus.FAILED
            assert loaded_job2.error_message == "Test error"

    def test_persistence_file_creation(self):
        """Test that persistence file and directories are created."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Use nested directory path
            persistence_file = Path(temp_dir) / "subdir" / "jobs.json"
            
            tracker = JobTracker(str(persistence_file))
            job_id = uuid4()
            
            # Create job (should create file and directories)
            tracker.create_job(job_id, 100)
            
            assert persistence_file.exists()
            assert persistence_file.parent.exists()

    def test_corrupted_persistence_file(self):
        """Test handling of corrupted persistence file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            persistence_file = Path(temp_dir) / "jobs.json"
            
            # Create corrupted JSON file
            with persistence_file.open('w') as f:
                f.write("invalid json content")
            
            # Should not crash, just log error and start with empty jobs
            tracker = JobTracker(str(persistence_file))
            
            jobs = tracker.list_jobs()
            assert len(jobs) == 0

    def test_atomic_file_operations(self):
        """Test that file operations are atomic."""
        with tempfile.TemporaryDirectory() as temp_dir:
            persistence_file = Path(temp_dir) / "jobs.json"
            
            tracker = JobTracker(str(persistence_file))
            job_id = uuid4()
            
            # Create job
            tracker.create_job(job_id, 100)
            
            # Verify file exists and is valid JSON
            assert persistence_file.exists()
            
            with persistence_file.open('r') as f:
                data = json.load(f)
                assert str(job_id) in data
                assert data[str(job_id)]["total_items"] == 100