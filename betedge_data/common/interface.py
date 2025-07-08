from abc import ABC, abstractmethod
from io import BytesIO

class IRequest(ABC):

    @abstractmethod
    def generate_object_key(self) -> str:
        """
        Returns a MinIO compatible object key 
        """

class IClient(ABC):

    @abstractmethod
    def get_data(self) -> BytesIO:
        """
        Returns a file wrapped in a Python BytesIO object.
        """