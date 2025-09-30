from pydantic import Field
from pydantic_settings import BaseSettings


class MinIOConfig(BaseSettings):
    """Configuration for MinIO S3-compatible object storage."""

    endpoint: str = Field(default="localhost:9000", description="MinIO server endpoint")
    access_key: str = Field(default="minioadmin", description="MinIO access key")
    secret_key: str = Field(default="minioadmin123", description="MinIO secret key")
    bucket: str = Field(
        default="betedge-data",
        description="MinIO bucket name for data storage",
    )
    secure: bool = Field(
        default=False,
        description="Use HTTPS for MinIO connections",
    )

    def get_minio_storage_options(self) -> dict:
        """Convert MinIO config vars to Polars-compatible storage options"""
        endpoint = self.endpoint
        secure = self.secure

        # Build the endpoint URL
        protocol = "https" if secure else "http"
        endpoint_url = f"{protocol}://{endpoint}"

        return {
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "aws_endpoint_url": endpoint_url,
            "aws_region": "us-east-1",
            "aws_allow_http": str(not secure).lower(),
        }


class GeneralConfig(BaseSettings):
    max_workers: int = Field(
        default=2,
        description="Number of threads to use. Should match the value in the config_0.properties for ThetaTerminal.",
    )
    http_timeout: int = Field(default=60)


class AppSettings(BaseSettings):
    minio: MinIOConfig = MinIOConfig()
    general: GeneralConfig = GeneralConfig()


global _app_settings
_app_settings = AppSettings()


def get_settings() -> AppSettings:
    return _app_settings
