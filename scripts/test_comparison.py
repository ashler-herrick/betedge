import time
import json
import orjson
from pydantic import BaseModel
from typing import List, Union

# Your data structure
test_data = {
    "header": {"id": 0, "latency_ms": 0, "error_type": "string", "error_msg": "string", "format": ["string"]},
    "response": [
        {
            "ticks": [[35100000, 13, 47, 134.6, 50, 11, 47, 134.9, 50, 20231110] * 100],  # Simulate larger dataset
            "contract": {"root": "AAPL", "expiration": 20231117, "strike": 50000, "right": "C"},
        }
        for _ in range(1000)  # 1000 response items
    ],
}


# Benchmark different approaches
def benchmark_json_only():
    json_str = json.dumps(test_data)
    start = time.time()
    for _ in range(100):
        data = json.loads(json_str)
    return time.time() - start


def benchmark_orjson():
    json_str = orjson.dumps(test_data)
    start = time.time()
    for _ in range(100):
        data = orjson.loads(json_str)
    return time.time() - start


def benchmark_pydantic():
    json_str = json.dumps(test_data)
    start = time.time()
    for _ in range(100):
        data = json.loads(json_str)
        validated = APIResponse(**data)
    return time.time() - start


if __name__ == "__main__":
    benchmark_json_only()
