from typing import Protocol
from io import BytesIO


class IRequest(Protocol):
    def generate_object_key(self) -> str:
        """Returns a MinIO compatible object key."""
        ...


class IClient(Protocol):
    def get_data(self, request: IRequest) -> BytesIO:
        """Returns a file wrapped in a Python BytesIO object."""
        ...
