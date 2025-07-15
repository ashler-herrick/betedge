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
    ExternalHistoricalOptionRequest,
    ExternalHistoricalStockRequest,
    ExternalEarningsRequest,
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
    title="BetEdge Historical Data API",
    description="REST API for processing historical options and stock data with date range support",
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
        "endpoints": ["/historical/option", "/historical/stock", "/earnings"],
        "docs": "/docs",
        "health": "/health",
    }


@app.post(
    "/historical/option",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Process historical option data with date range support",
    description="Fetch filtered historical option data for a date range and upload each day to object storage",
)
async def process_historical_option(request: ExternalHistoricalOptionRequest):
    """Process historical option data request with date range support."""
    request_id = uuid4()
    logger.info(f"Received historical option request {request_id}: {request.root}")

    try:
        await service.process_request(request, request_id)
        return {"status": "success", "request_id": str(request_id)}
    except Exception as e:
        logger.error(f"Error processing historical option request {request_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process historical option request: {str(e)}",
        )


@app.post(
    "/historical/stock",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Process historical stock data with date range support",
    description="Fetch historical stock quote data for a date range and upload each day to object storage",
)
async def process_historical_stock(request: ExternalHistoricalStockRequest):
    """Process historical stock data request with date range support."""
    request_id = uuid4()
    logger.info(f"Received historical stock request {request_id}: {request.root}")

    try:
        await service.process_request(request, request_id)
        return {"status": "success", "request_id": str(request_id)}
    except Exception as e:
        logger.error(f"Error processing historical stock request {request_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process historical stock request: {str(e)}",
        )


@app.post(
    "/earnings",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Process earnings data with date range support",
    description="Fetch earnings data for a date range and upload to object storage organized by month",
)
async def process_earnings(request: ExternalEarningsRequest):
    """Process earnings data request with date range support."""
    request_id = uuid4()
    logger.info(f"Received earnings request {request_id} from {request.start_date} to {request.end_date}")

    try:
        await service.process_request(request, request_id)
        return {"status": "success", "request_id": str(request_id)}
    except Exception as e:
        logger.error(f"Error processing earnings request {request_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process earnings request: {str(e)}",
        )



# Additional utility endpoints


@app.get("/config")
async def get_configuration():
    """Get current service configuration (non-sensitive fields only)."""
    return {
        "message": "Configuration endpoint available for historical data service",
        "storage_type": "MinIO S3-compatible object storage",
        "storage_patterns": {
            "options": "historical-options/{symbol}/{year}/{month}/{day}/{filename}.parquet",
            "stocks": "historical-stock/{symbol}/{year}/{month}/{day}/{filename}.parquet",
            "earnings": "earnings/{year}/{month}/earnings-{year}-{month}.parquet",
        },
    }


# Storage utility endpoints removed - not compatible with simplified service


if __name__ == "__main__":
    import uvicorn

    # Run the application
    uvicorn.run("betedge_data.manager.api:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
