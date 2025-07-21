# Phase 1: Async Background Jobs Refactoring Plan

## Overview

This document outlines Phase 1 of converting the BetEdge data processing service from synchronous blocking operations to async background jobs. The goal is to make long-running requests (like yearly option data) non-blocking while maintaining simplicity and avoiding external dependencies.

## Current Architecture Analysis

### Request Flow
1. **API Endpoint** receives external request → `POST /historical/option`
2. **Service Layer** processes request synchronously → `DataProcessingService.process_request()`
3. **Client Layer** fetches data sequentially → `HistoricalOptionClient.get_data()`
4. **Storage Layer** publishes to MinIO → `MinIOPublisher.publish()`

### Identified Bottlenecks
- **Sequential processing**: 200+ requests for yearly data processed one-by-one
- **Blocking I/O**: HTTP calls to ThetaData API block entire request
- **No progress tracking**: Client has no visibility into long-running operations
- **Resource waste**: Single-threaded processing doesn't utilize available concurrency

### Key Files to Modify
- `betedge_data/manager/api.py` - API endpoints
- `betedge_data/manager/service.py` - Service layer processing
- `betedge_data/manager/models.py` - Add job tracking models

## Phase 1 Implementation Plan

### 1. Job Management System

#### Create Job Tracker Class
```python
# betedge_data/manager/job_tracker.py
from typing import Dict, Optional
from dataclasses import dataclass
from uuid import UUID
from datetime import datetime
from enum import Enum

class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class JobInfo:
    job_id: UUID
    status: JobStatus
    total_items: int
    completed_items: int
    error_message: Optional[str]
    created_at: datetime
    updated_at: datetime
    
    @property
    def progress_percentage(self) -> float:
        return (self.completed_items / self.total_items) * 100 if self.total_items > 0 else 0

class JobTracker:
    """In-memory job tracking with optional file persistence."""
    
    def __init__(self):
        self._jobs: Dict[UUID, JobInfo] = {}
    
    def create_job(self, job_id: UUID, total_items: int) -> JobInfo:
        """Create a new job entry."""
        pass
    
    def update_progress(self, job_id: UUID, completed_items: int) -> None:
        """Update job progress."""
        pass
    
    def mark_completed(self, job_id: UUID) -> None:
        """Mark job as completed."""
        pass
    
    def mark_failed(self, job_id: UUID, error: str) -> None:
        """Mark job as failed with error message."""
        pass
    
    def get_job(self, job_id: UUID) -> Optional[JobInfo]:
        """Get job information."""
        pass
```

### 2. Modified Service Layer

#### Update DataProcessingService
```python
# betedge_data/manager/service.py - Key Changes

class DataProcessingService:
    def __init__(self, force_refresh: bool = False):
        self.force_refresh = force_refresh
        self.publisher = MinIOPublisher()
        self.job_tracker = JobTracker()  # NEW
        self._background_tasks: Set[asyncio.Task] = set()  # NEW
        logger.info("DataProcessingService initialized")

    async def process_request(self, request: ExternalBaseRequest, request_id: UUID) -> None:
        """Create background task for processing instead of blocking."""
        # Create job entry
        individual_requests = request.get_subrequests()
        self.job_tracker.create_job(request_id, len(individual_requests))
        
        # Start background processing
        task = asyncio.create_task(self._process_request_background(request, request_id))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        
        logger.info(f"Background job {request_id} started with {len(individual_requests)} items")

    async def _process_request_background(self, request: ExternalBaseRequest, request_id: UUID) -> None:
        """Background processing with progress tracking."""
        try:
            self.job_tracker.update_status(request_id, JobStatus.RUNNING)
            individual_requests = request.get_subrequests()
            client = request.get_client()
            
            completed = 0
            for req in individual_requests:
                try:
                    # Process single request (existing logic)
                    object_key = req.generate_object_key()
                    
                    if not self.force_refresh and self.publisher.file_exists(object_key):
                        logger.info(f"File exists for {getattr(req, 'root', 'unknown')} - skipping")
                        completed += 1
                        self.job_tracker.update_progress(request_id, completed)
                        continue
                    
                    result = client.get_data(req)
                    await self.publisher.publish(result, object_key)
                    
                    completed += 1
                    self.job_tracker.update_progress(request_id, completed)
                    
                    logger.info(f"Processed {completed}/{len(individual_requests)} items for job {request_id}")
                    
                except NoDataAvailableError:
                    completed += 1
                    self.job_tracker.update_progress(request_id, completed)
                    continue
                except Exception as e:
                    logger.error(f"Failed to process item in job {request_id}: {e}")
                    completed += 1
                    self.job_tracker.update_progress(request_id, completed)
                    continue
            
            self.job_tracker.mark_completed(request_id)
            logger.info(f"Background job {request_id} completed successfully")
            
        except Exception as e:
            self.job_tracker.mark_failed(request_id, str(e))
            logger.error(f"Background job {request_id} failed: {e}")

    def get_job_status(self, job_id: UUID) -> Optional[JobInfo]:
        """Get job status for API endpoint."""
        return self.job_tracker.get_job(job_id)
```

### 3. API Endpoint Changes

#### Update API Endpoints
```python
# betedge_data/manager/api.py - Key Changes

@app.post(
    "/historical/option",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit historical option data request for background processing",
)
async def process_historical_option(request: ExternalHistoricalOptionRequest):
    """Submit request and return immediately with job ID."""
    request_id = uuid4()
    logger.info(f"Received historical option request {request_id}: {request.root}")

    try:
        # Start background processing (non-blocking)
        await service.process_request(request, request_id)
        
        return {
            "status": "accepted", 
            "job_id": str(request_id),
            "message": "Request submitted for background processing",
            "status_url": f"/jobs/{request_id}"
        }
    except Exception as e:
        logger.error(f"Error submitting request {request_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit request: {str(e)}",
        )

@app.get(
    "/jobs/{job_id}",
    summary="Get job status and progress",
)
async def get_job_status(job_id: str):
    """Get status of background job."""
    try:
        job_uuid = UUID(job_id)
        job_info = service.get_job_status(job_uuid)
        
        if not job_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id} not found"
            )
        
        return {
            "job_id": str(job_info.job_id),
            "status": job_info.status,
            "progress": {
                "completed": job_info.completed_items,
                "total": job_info.total_items,
                "percentage": job_info.progress_percentage
            },
            "created_at": job_info.created_at.isoformat(),
            "updated_at": job_info.updated_at.isoformat(),
            "error_message": job_info.error_message
        }
        
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid job ID format"
        )
    except Exception as e:
        logger.error(f"Error getting job status {job_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get job status: {str(e)}"
        )
```

## Implementation Steps

### Step 1: Create Job Management Infrastructure
1. Create `betedge_data/manager/job_tracker.py` with JobTracker class
2. Add job status models and enums
3. Implement in-memory job storage with optional file persistence

### Step 2: Modify Service Layer
1. Update `DataProcessingService.__init__()` to include JobTracker
2. Convert `process_request()` to start background tasks
3. Add `_process_request_background()` method with progress tracking
4. Add job status getter method

### Step 3: Update API Endpoints
1. Modify existing endpoints to return job IDs immediately
2. Add new `/jobs/{job_id}` endpoint for status checking
3. Update response models to include job information

### Step 4: Add Error Handling
1. Ensure background tasks don't crash the service
2. Add proper error logging and job failure tracking
3. Handle edge cases (duplicate requests, invalid job IDs)

## Testing Strategy

### Unit Tests
- JobTracker functionality (create, update, status transitions)
- Background task creation and completion
- Error handling in background processing

### Integration Tests
- End-to-end request submission and status checking
- Multiple concurrent background jobs
- Job persistence across service restarts (if implemented)

### Performance Tests
- Memory usage with many concurrent jobs
- Response time improvements for large requests
- Background task throughput

## Benefits Achieved

### Immediate Benefits
- **Non-blocking requests**: API returns immediately with job ID
- **Progress visibility**: Clients can monitor long-running operations
- **Better resource utilization**: Service can handle multiple requests
- **Improved user experience**: No timeout issues on large requests

### Maintained Simplicity
- **No external dependencies**: Pure Python asyncio solution
- **Minimal code changes**: Existing logic largely preserved
- **Easy rollback**: Can revert to synchronous processing if needed
- **Simple deployment**: No additional services to manage

## Future Enhancements (Phase 2)

### Concurrent Processing
- Process multiple requests in parallel using `asyncio.gather()`
- Add configurable concurrency limits
- Implement request batching for efficiency

### Enhanced SDK
- Add polling functionality to client SDK
- Implement `wait_for_completion()` with progress callbacks
- Add retry mechanisms for failed operations

## Migration Considerations

### Backwards Compatibility
- Keep existing response format for clients expecting synchronous behavior
- Add feature flag to toggle between sync/async modes during transition
- Maintain existing error handling patterns

### Deployment Strategy
1. Deploy with async disabled by default
2. Enable for specific endpoints/clients
3. Gradually migrate all endpoints
4. Remove synchronous fallback after validation

### Monitoring
- Add metrics for job completion rates
- Monitor background task memory usage
- Track API response times and throughput improvements