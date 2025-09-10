"""
FastAPI application for data processing service.

This module defines the REST API endpoints for processing historical and live
data requests through the ThetaData clients.
"""

import logging
from typing import Optional
from contextlib import asynccontextmanager
from uuid import uuid4, UUID

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

from betedge_data.manager.external_models import (
    ExternalHistoricalOptionRequest,
    ExternalHistoricalStockRequest,
    ExternalEarningsRequest,
)
from betedge_data.manager.service import DataProcessingService
from betedge_data.manager.job_tracker import JobStatus

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan - startup and shutdown."""
    global service

    # Startup
    logger.info("Starting data processing service")

    service = DataProcessingService()

    yield

    # Shutdown
    logger.info("Shutting down data processing service")
    if service:
        await service.close()


# Create FastAPI application
app = FastAPI(
    title="BetEdge Historical Data API",
    description="REST API for processing historical options and stock data with date range support",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled exception in {request.method} {request.url}: {exc}", exc_info=True)

    error_response = {
        "error_code": "INTERNAL_ERROR",
        "message": "An internal error occurred while processing the request",
        "details": str(exc),
    }

    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=error_response)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "betedge-historical-data"}


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "BetEdge Historical Data API",
        "version": "1.0.0",
        "endpoints": ["/historical/option", "/historical/stock", "/earnings", "/jobs", "/jobs/{job_id}"],
        "docs": "/docs",
        "health": "/health",
    }


@app.post(
    "/historical/option",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit historical option data request for background processing",
    description="Submit historical option data request and return job ID for tracking",
)
async def process_historical_option(request: ExternalHistoricalOptionRequest):
    """Submit historical option data request for background processing."""
    request_id = uuid4()
    logger.info(f"Received historical option request {request_id}: {request.root}")

    try:
        # Start background processing (non-blocking)
        await service.process_request(request, request_id)

        return {
            "status": "accepted",
            "job_id": str(request_id),
            "message": "Request submitted for background processing",
            "status_url": f"/jobs/{request_id}",
        }
    except Exception as e:
        logger.error(f"Error submitting request {request_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit request: {str(e)}",
        )


@app.post(
    "/historical/stock",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit historical stock data request for background processing",
    description="Submit historical stock data request and return job ID for tracking",
)
async def process_historical_stock(request: ExternalHistoricalStockRequest):
    """Submit historical stock data request for background processing."""
    request_id = uuid4()
    logger.info(f"Received historical stock request {request_id}: {request.root}")

    try:
        # Start background processing (non-blocking)
        await service.process_request(request, request_id)

        return {
            "status": "accepted",
            "job_id": str(request_id),
            "message": "Request submitted for background processing",
            "status_url": f"/jobs/{request_id}",
        }
    except Exception as e:
        logger.error(f"Error submitting request {request_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit request: {str(e)}",
        )


@app.post(
    "/earnings",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit earnings data request for background processing",
    description="Submit earnings data request and return job ID for tracking",
)
async def process_earnings(request: ExternalEarningsRequest):
    """Submit earnings data request for background processing."""
    request_id = uuid4()
    logger.info(f"Received earnings request {request_id} from {request.start_date} to {request.end_date}")

    try:
        # Start background processing (non-blocking)
        await service.process_request(request, request_id)

        return {
            "status": "accepted",
            "job_id": str(request_id),
            "message": "Request submitted for background processing",
            "status_url": f"/jobs/{request_id}",
        }
    except Exception as e:
        logger.error(f"Error submitting request {request_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit request: {str(e)}",
        )


@app.get(
    "/jobs",
    summary="List all jobs",
    description="Get a list of all active and historical jobs with optional filtering",
)
async def list_jobs(
    limit: Optional[int] = None,
    job_status: Optional[str] = None,
):
    """Get list of all jobs with optional filtering."""
    try:
        # Validate and convert status parameter
        status_filter = None
        if job_status:
            try:
                status_filter = JobStatus(job_status.lower())
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, 
                    detail=f"Invalid status '{job_status}'. Valid statuses: {[s.value for s in JobStatus]}"
                )

        jobs = service.get_all_jobs(limit=limit, status=status_filter)
        
        return {
            "total": len(jobs),
            "limit": limit,
            "status_filter": job_status,
            "jobs": [
                {
                    "job_id": str(job.job_id),
                    "status": job.status.value,
                    "progress": {
                        "completed": job.completed_items,
                        "total": job.total_items,
                        "percentage": job.progress_percentage,
                    },
                    "created_at": job.created_at.isoformat(),
                    "updated_at": job.updated_at.isoformat(),
                    "error_message": job.error_message,
                }
                for job in jobs
            ]
        }

    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to list jobs: {str(e)}"
        )


@app.get(
    "/jobs/{job_id}",
    summary="Get job status and progress",
    description="Monitor the status and progress of a background job",
)
async def get_job_status(job_id: str):
    """Get status of background job."""
    try:
        job_uuid = UUID(job_id)
        job_info = service.get_job_status(job_uuid)

        if not job_info:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job {job_id} not found")

        return {
            "job_id": str(job_info.job_id),
            "status": job_info.status,
            "progress": {
                "completed": job_info.completed_items,
                "total": job_info.total_items,
                "percentage": job_info.progress_percentage,
            },
            "created_at": job_info.created_at.isoformat(),
            "updated_at": job_info.updated_at.isoformat(),
            "error_message": job_info.error_message,
        }

    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid job ID format")
    except Exception as e:
        logger.error(f"Error getting job status {job_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to get job status: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn

    # Run the application
    uvicorn.run("betedge_data.manager.api:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
