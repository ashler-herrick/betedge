import threading
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from io import BytesIO
from enum import Enum

import pyarrow as pa


class ReturnType(Enum):
    CSV = "csv"
    JSON = "json"


class Schema(Enum):
    OPTION_EOD = "option_eod"
    OPTION_QUOTE = "option_quote"
    STOCK_EOD = "stock_eod"
    STOCK_QUOTE = "stock_quote"
    EARNINGS = "earnings"


@dataclass(slots=True)
class FileWriteJob:
    """
    The FileWriteJob represents a request for a new file coming from the client, as the name implies.
    It contains a object_key to ultimate use when writing and a BytesIO wrapped parquet file.
    """

    object_key: str
    total_items: int
    completed_items: int = 0
    completed: bool = False
    tables: List[pa.table] = field(default_factory=list)
    byte_wrapper: Optional[BytesIO] = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add_table(self, table: pa.table) -> None:
        with self._lock:
            self.tables.append(table)
            self.completed_items += 1
            if self.completed_items == self.total_items:
                self.completed = True


@dataclass(slots=True)
class HTTPJob:
    """ """

    url: str
    schema: Schema
    return_type: ReturnType
    file_write_job: FileWriteJob
    headers: Optional[Dict[str, str]] = None
    # Variables to hold the response
    csv_buffer: Optional[BytesIO] = None
    json: Optional[Dict[str, Any] | Any] = None
