"""
FastAPI application for data processing service.

This module defines the REST API endpoints for processing historical and live
data requests through the ThetaData clients.
"""

import logging
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
    service = DataProcessingService()
    
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
    lifespan=lifespan
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
        error_code="INTERNAL_ERROR",
        message="An internal error occurred while processing the request",
        details=str(exc)
    )
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=error_response.dict()
    )


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
        "endpoints": [
            "/historical/option"
        ],
        "docs": "/docs",
        "health": "/health"
    }


@app.post(
    "/historical/option",
    response_model=DataProcessingResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Process historical option data with date range support",
    description="Fetch filtered historical option data for a date range and publish each day to Kafka"
)
async def process_historical_option(
    request: HistoricalOptionRequest
) -> DataProcessingResponse:
    """
    Process historical option data request with date range support.
    
    Fetches historical option data with time-matched underlying prices,
    applies moneyness and DTE filtering via Rust parser, and publishes
    each day's Parquet data to a symbol-specific Kafka topic.
    
    Date ranges are automatically split into individual days, with each
    day published as a separate message to maintain ordering.
    
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
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=response.message
            )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing historical option request {request_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process historical option request: {str(e)}"
        )



# Additional utility endpoints

@app.get("/config")
async def get_configuration():
    """Get current service configuration (non-sensitive fields only)."""
    return {
        "message": "Configuration endpoint available for historical options service",
        "kafka_message_size_limit": "10MB",
        "topic_naming_pattern": "historical-option-{symbol}-quote-{interval}"
    }


@app.get("/topics")
async def get_kafka_topics():
    """Get list of example Kafka topics used by the service."""
    from .models import generate_topic_name, interval_to_string
    
    # Show some example topics
    example_intervals = [900000, 3600000, 86400000]  # 15m, 1h, 1d
    example_symbols = ["AAPL", "SPY", "TSLA"]
    
    topics = {}
    for symbol in example_symbols:
        for interval in example_intervals:
            topic_name = generate_topic_name(symbol, interval)
            topics[f"{symbol}_{interval_to_string(interval)}"] = topic_name
    
    return {
        "topic_pattern": "historical-option-{symbol}-quote-{interval}",
        "example_topics": topics
    }


if __name__ == "__main__":
    import uvicorn
    
    # Run the application
    uvicorn.run(
        "betedge_data.manager.api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )