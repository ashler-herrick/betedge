"""
FastAPI application for data processing service.

This module defines the REST API endpoints for processing historical and live
data requests through the ThetaData clients.
"""

import logging
import os
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from betedge_data.manager.models import (
    DataProcessingResponse,
    ErrorResponse,
    HistoricalOptionRequest,
)
from betedge_data.manager.service import DataProcessingService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Global service instance
service: DataProcessingService = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan - startup and shutdown."""
    global service

    # Startup
    logger.info("Starting data processing service")

    # Check for force refresh environment variable
    force_refresh = os.getenv("FORCE_REFRESH", "false").lower() in ("true", "1", "yes")

    if force_refresh:
        logger.info("Force refresh mode enabled via FORCE_REFRESH environment variable")

    service = DataProcessingService(force_refresh=force_refresh)

    yield

    # Shutdown
    logger.info("Shutting down data processing service")
    if service:
        await service.close()


# Create FastAPI application
app = FastAPI(
    title="BetEdge Historical Options API",
    description="REST API for processing historical option data with date range support",
    version="1.0.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure as needed for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled exception in {request.method} {request.url}: {exc}", exc_info=True)

    error_response = ErrorResponse(
        error_code="INTERNAL_ERROR", message="An internal error occurred while processing the request", details=str(exc)
    )

    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=error_response.dict())


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "betedge-historical-options"}


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "BetEdge Historical Options API",
        "version": "1.0.0",
        "endpoints": ["/historical/option"],
        "docs": "/docs",
        "health": "/health",
    }


@app.post(
    "/historical/option",
    response_model=DataProcessingResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Process historical option data with date range support",
    description="Fetch filtered historical option data for a date range and upload each day to object storage",
)
async def process_historical_option(request: HistoricalOptionRequest) -> DataProcessingResponse:
    """
    Process historical option data request with date range support.

    Fetches historical option data with time-matched underlying prices,
    applies moneyness and DTE filtering via Rust parser, and uploads
    each day's Parquet data to MinIO object storage.

    Date ranges are automatically split into individual days, with each
    day uploaded as a separate file with hierarchical organization.

    Args:
        request: Historical option data request parameters

    Returns:
        Processing response with metadata about the operation

    Raises:
        HTTPException: If request validation fails or processing errors occur
    """
    request_id = uuid4()
    logger.info(f"Received historical option request {request_id}: {request.root}")

    try:
        response = await service.process_historical_option_request(request, request_id)

        if response.status == "error":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=response.message)

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing historical option request {request_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process historical option request: {str(e)}",
        )


# Additional utility endpoints


@app.get("/config")
async def get_configuration():
    """Get current service configuration (non-sensitive fields only)."""
    return {
        "message": "Configuration endpoint available for historical options service",
        "storage_type": "MinIO S3-compatible object storage",
        "storage_path_pattern": "historical-options/{symbol}/{year}/{month}/{day}/{filename}.parquet",
    }


@app.get("/storage/stats")
async def get_storage_stats():
    """Get MinIO storage statistics."""
    if not service or not service.minio_publisher:
        raise HTTPException(status_code=503, detail="MinIO publisher not available")

    try:
        stats = service.minio_publisher.get_bucket_stats()
        stats["force_refresh_mode"] = service.force_refresh
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get storage stats: {str(e)}")


@app.get("/storage/symbol/{symbol}")
async def list_symbol_files(symbol: str, start_date: str = None, end_date: str = None, limit: int = 100):
    """List files for a symbol with optional date filtering."""
    if not service or not service.minio_publisher:
        raise HTTPException(status_code=503, detail="MinIO publisher not available")

    try:
        files = service.minio_publisher.list_files_for_symbol(
            root=symbol.upper(), start_date=start_date, end_date=end_date, limit=limit
        )
        return {"symbol": symbol.upper(), "files": files, "count": len(files)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list files for {symbol}: {str(e)}")


@app.delete("/storage/symbol/{symbol}")
async def delete_symbol_files(symbol: str):
    """Delete all files for a symbol (use with caution)."""
    if not service or not service.minio_publisher:
        raise HTTPException(status_code=503, detail="MinIO publisher not available")

    try:
        # Get all files for the symbol
        files = service.minio_publisher.list_files_for_symbol(root=symbol.upper())

        deleted_count = 0
        for file_info in files:
            if service.minio_publisher.delete_file(file_info["object_name"]):
                deleted_count += 1

        return {
            "symbol": symbol.upper(),
            "deleted_files": deleted_count,
            "total_files": len(files),
            "message": f"Deleted {deleted_count}/{len(files)} files for {symbol}",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete files for {symbol}: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    # Run the application
    uvicorn.run("betedge_data.manager.api:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
