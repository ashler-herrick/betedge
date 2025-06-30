#!/usr/bin/env python3
"""
Benchmark comparing Rust vs Python (PyArrow) for IPC and Parquet serialization.

Tests 4 head-to-head comparisons:
1. Python PyArrow IPC vs Rust IPC
2. Python PyArrow Parquet vs Rust Parquet
"""

import json
import time
import tracemalloc
import io
from datetime import date, timedelta
from typing import Tuple, Optional

# PyArrow imports
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

import fast_parser




def generate_realistic_test_data(num_contracts: int) -> tuple[str, str]:
    """Generate ThetaData-like JSON with realistic option contracts and matching stock data."""
    contracts = []
    stock_ticks = []
    
    # Realistic parameters
    base_symbols = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]
    base_prices = [150.0, 300.0, 2500.0, 200.0, 3000.0]
    
    # Collect all unique timestamps from option data for stock generation
    all_timestamps = set()
    
    for i in range(num_contracts):
        symbol_idx = i % len(base_symbols)
        symbol = base_symbols[symbol_idx]
        base_price = base_prices[symbol_idx]
        
        # Generate realistic strikes around current price (±50%)
        strike_offset = (i % 40 - 20) * 0.025  # -50% to +50% in 2.5% increments
        strike_price = base_price * (1 + strike_offset)
        strike_minor = int(strike_price * 10000)  # Convert to minor units
        
        # Generate realistic expirations (1-90 days out)
        dte = 1 + (i % 90)
        base_date = date(2023, 11, 17)
        exp_date = base_date + timedelta(days=dte)
        expiration = int(exp_date.strftime("%Y%m%d"))
        
        # Alternate between calls and puts
        right = "C" if i % 2 == 0 else "P"
        
        # Generate realistic bid/ask spread
        bid_price = strike_price - 0.25
        ask_price = strike_price + 0.25
        
        # Multiple ticks per contract (1-3 ticks)
        num_ticks = 32
        ticks = []
        for tick_idx in range(num_ticks):
            time_offset = tick_idx * 900_000  # 15 minutes apart
            timestamp = 34200000 + time_offset
            all_timestamps.add(timestamp)  # Collect for stock data
            ticks.append([
                timestamp,               # ms_of_day
                10 + tick_idx,           # bid_size
                47,                      # bid_exchange
                bid_price + tick_idx * 0.01,  # bid_price
                50,                      # bid_condition
                10 + tick_idx,           # ask_size
                47,                      # ask_exchange
                ask_price + tick_idx * 0.01,  # ask_price
                50,                      # ask_condition
                expiration               # date
            ])
        
        contract = {
            "ticks": ticks,
            "contract": {
                "root": symbol,
                "expiration": expiration,
                "strike": strike_minor,
                "right": right
            }
        }
        contracts.append(contract)
    
    # Generate matching stock data for unique timestamps
    # Use primary symbol (AAPL) for stock data  
    primary_price = base_prices[0]  # AAPL price
    
    for timestamp in sorted(all_timestamps):
        # Add some realistic price movement
        price_variance = 0.5 * (hash(timestamp) % 100 - 50) / 100  # ±0.5%
        current_price = primary_price * (1 + price_variance)
        bid_price = current_price - 0.05
        ask_price = current_price + 0.05
        
        stock_ticks.append([
            timestamp,                    # ms_of_day
            200,                         # bid_size  
            47,                          # bid_exchange
            bid_price,                   # bid_price
            50,                          # bid_condition
            200,                         # ask_size
            47,                          # ask_exchange  
            ask_price,                   # ask_price
            50,                          # ask_condition
            20231117                     # date
        ])
    
    # Create option response
    option_response = {
        "header": {
            "latency_ms": 50,
            "format": ["ms_of_day","bid_size","bid_exchange","bid","bid_condition","ask_size","ask_exchange","ask","ask_condition","date"]
        },
        "response": contracts
    }
    
    # Create stock response
    stock_response = {
        "header": {
            "latency_ms": 25,
            "format": ["ms_of_day","bid_size","bid_exchange","bid","bid_condition","ask_size","ask_exchange","ask","ask_condition","date"]
        },
        "response": stock_ticks
    }
    
    return json.dumps(option_response), json.dumps(stock_response)


def python_filter_to_ipc_bytes(
    option_json: str,
    stock_json: str, 
    current_date_int: int, 
    max_dte: int, 
    base_pct: float
) -> bytes:
    """Python implementation using PyArrow for IPC serialization with time-matched underlying prices."""
    # Parse both JSONs
    option_data = json.loads(option_json)
    stock_data = json.loads(stock_json)
    
    # Build price index from stock data
    price_index = {}
    for stock_tick in stock_data["response"]:
        timestamp = stock_tick[0]
        bid = stock_tick[3]
        ask = stock_tick[7]
        midpoint = (bid + ask) / 2.0
        price_index[timestamp] = midpoint
    
    # Convert current date
    current_date_str = str(current_date_int)
    current_date = date(
        int(current_date_str[:4]),
        int(current_date_str[4:6]),
        int(current_date_str[6:8])
    )
    
    # Collect all filtered ticks
    filtered_ticks = []
    
    for entry in option_data["response"]:
        # Parse expiration date
        exp_str = str(entry["contract"]["expiration"])
        try:
            exp_date = date(
                int(exp_str[:4]),
                int(exp_str[4:6]),
                int(exp_str[6:8])
            )
        except (ValueError, IndexError):
            continue
            
        # Calculate DTE
        dte = (exp_date - current_date).days
        if dte <= 0 or dte > max_dte:
            continue
            
        # Calculate moneyness threshold (dynamic based on DTE)
        strike_dollars = entry["contract"]["strike"] / 10000.0
        threshold = base_pct * (dte ** 0.5)
        
        # Process each tick with time-matched underlying price
        for tick in entry["ticks"]:
            timestamp = tick[0]
            
            # Get time-matched underlying price
            underlying = price_index.get(timestamp)
            if underlying is None:
                continue  # Skip if no matching underlying price
            
            price_diff = abs(strike_dollars - underlying)
            
            # Apply moneyness filter
            if price_diff <= underlying * threshold:
                tick_data = {
                    'ms_of_day': tick[0],
                    'bid_size': tick[1],
                    'bid_exchange': tick[2],
                    'bid_price': float(tick[3]),
                    'bid_condition': tick[4],
                    'ask_size': tick[5],
                    'ask_exchange': tick[6],
                    'ask_price': float(tick[7]),
                    'ask_condition': tick[8],
                    'date': tick[9],
                    'root': entry["contract"]["root"],
                    'expiration': entry["contract"]["expiration"],
                    'strike': entry["contract"]["strike"],
                    'right': entry["contract"]["right"]
                }
                filtered_ticks.append(tick_data)
    
    if not filtered_ticks:
        # Return empty Arrow IPC bytes
        schema = pa.schema([
            ('ms_of_day', pa.uint32()),
            ('bid_size', pa.uint16()),
            ('bid_exchange', pa.uint8()),
            ('bid_price', pa.float32()),
            ('bid_condition', pa.uint8()),
            ('ask_size', pa.uint16()),
            ('ask_exchange', pa.uint8()),
            ('ask_price', pa.float32()),
            ('ask_condition', pa.uint8()),
            ('date', pa.uint32()),
            ('root', pa.string()),
            ('expiration', pa.uint32()),
            ('strike', pa.uint32()),
            ('right', pa.string())
        ])
        empty_batch = pa.record_batch([], schema)
        buffer = io.BytesIO()
        with ipc.new_stream(buffer, schema) as writer:
            writer.write_batch(empty_batch)
        return buffer.getvalue()
    
    # Create PyArrow Table
    table = pa.Table.from_pylist(filtered_ticks)
    
    # Serialize to Arrow IPC bytes
    buffer = io.BytesIO()
    with ipc.new_stream(buffer, table.schema) as writer:
        writer.write_table(table)
    
    return buffer.getvalue()


def python_filter_to_parquet_bytes(
    option_json: str,
    stock_json: str, 
    current_date_int: int, 
    max_dte: int, 
    base_pct: float,
    compression: Optional[str] = None
) -> bytes:
    """Python implementation using PyArrow for Parquet serialization with time-matched underlying prices."""
    # Parse both JSONs (same logic as IPC version)
    option_data = json.loads(option_json)
    stock_data = json.loads(stock_json)
    
    # Build price index from stock data
    price_index = {}
    for stock_tick in stock_data["response"]:
        timestamp = stock_tick[0]
        bid = stock_tick[3]
        ask = stock_tick[7]
        midpoint = (bid + ask) / 2.0
        price_index[timestamp] = midpoint
    
    # Convert current date
    current_date_str = str(current_date_int)
    current_date = date(
        int(current_date_str[:4]),
        int(current_date_str[4:6]),
        int(current_date_str[6:8])
    )
    
    # Collect all filtered ticks
    filtered_ticks = []
    
    for entry in option_data["response"]:
        # Parse expiration date
        exp_str = str(entry["contract"]["expiration"])
        try:
            exp_date = date(
                int(exp_str[:4]),
                int(exp_str[4:6]),
                int(exp_str[6:8])
            )
        except (ValueError, IndexError):
            continue
            
        # Calculate DTE
        dte = (exp_date - current_date).days
        if dte <= 0 or dte > max_dte:
            continue
            
        # Calculate moneyness threshold (dynamic based on DTE)
        strike_dollars = entry["contract"]["strike"] / 10000.0
        threshold = base_pct * (dte ** 0.5)
        
        # Process each tick with time-matched underlying price
        for tick in entry["ticks"]:
            timestamp = tick[0]
            
            # Get time-matched underlying price
            underlying = price_index.get(timestamp)
            if underlying is None:
                continue  # Skip if no matching underlying price
            
            price_diff = abs(strike_dollars - underlying)
            
            # Apply moneyness filter
            if price_diff <= underlying * threshold:
                tick_data = {
                    'ms_of_day': tick[0],
                    'bid_size': tick[1],
                    'bid_exchange': tick[2],
                    'bid_price': float(tick[3]),
                    'bid_condition': tick[4],
                    'ask_size': tick[5],
                    'ask_exchange': tick[6],
                    'ask_price': float(tick[7]),
                    'ask_condition': tick[8],
                    'date': tick[9],
                    'root': entry["contract"]["root"],
                    'expiration': entry["contract"]["expiration"],
                    'strike': entry["contract"]["strike"],
                    'right': entry["contract"]["right"]
                }
                filtered_ticks.append(tick_data)
    
    if not filtered_ticks:
        # Return empty Parquet bytes
        schema = pa.schema([
            ('ms_of_day', pa.uint32()),
            ('bid_size', pa.uint16()),
            ('bid_exchange', pa.uint8()),
            ('bid_price', pa.float32()),
            ('bid_condition', pa.uint8()),
            ('ask_size', pa.uint16()),
            ('ask_exchange', pa.uint8()),
            ('ask_price', pa.float32()),
            ('ask_condition', pa.uint8()),
            ('date', pa.uint32()),
            ('root', pa.string()),
            ('expiration', pa.uint32()),
            ('strike', pa.uint32()),
            ('right', pa.string())
        ])
        empty_batch = pa.record_batch([], schema)
        buffer = io.BytesIO()
        pq.write_table(pa.Table.from_batches([empty_batch]), buffer, compression=compression)
        return buffer.getvalue()
    
    # Create PyArrow Table
    table = pa.Table.from_pylist(filtered_ticks)
    
    # Serialize to Parquet bytes
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression=compression)
    
    return buffer.getvalue()


def benchmark_implementation(name: str, func, *args) -> Tuple[bytes, float, float]:
    """Benchmark a function with timing and memory measurement."""
    # Start memory tracing
    tracemalloc.start()
    
    # Time the execution
    start_time = time.perf_counter()
    result = func(*args)
    end_time = time.perf_counter()
    
    # Get peak memory usage
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    execution_time = end_time - start_time
    peak_memory_mb = peak / (1024 * 1024)
    
    return result, execution_time, peak_memory_mb


def count_ticks_from_ipc_bytes(ipc_bytes) -> int:
    """Count number of ticks in IPC bytes."""
    try:
        # Handle both bytes and list types (Rust returns list, Python returns bytes)
        if isinstance(ipc_bytes, list):
            ipc_bytes = bytes(ipc_bytes)
            
        buffer = io.BytesIO(ipc_bytes)
        reader = ipc.open_stream(buffer)
        total_rows = 0
        for batch in reader:
            total_rows += batch.num_rows
        return total_rows
    except:
        return 0


def count_ticks_from_parquet_bytes(parquet_bytes) -> int:
    """Count number of ticks in Parquet bytes."""
    try:
        # Handle both bytes and list types (Rust returns list, Python returns bytes)
        if isinstance(parquet_bytes, list):
            parquet_bytes = bytes(parquet_bytes)
            
        buffer = io.BytesIO(parquet_bytes)
        table = pq.read_table(buffer)
        return table.num_rows
    except:
        return 0


def benchmark_ipc_read_performance(ipc_bytes) -> Tuple[pa.Table, float]:
    """Benchmark reading IPC bytes back to PyArrow Table."""
    start_time = time.perf_counter()
    
    # Handle both bytes and list types (Rust returns list, Python returns bytes)
    if isinstance(ipc_bytes, list):
        ipc_bytes = bytes(ipc_bytes)
    
    buffer = io.BytesIO(ipc_bytes)
    reader = ipc.open_stream(buffer)
    
    # Read all batches into a table
    batches = []
    for batch in reader:
        batches.append(batch)
    
    if batches:
        table = pa.Table.from_batches(batches)
    else:
        # Empty table with schema from reader
        table = pa.Table.from_arrays([], schema=reader.schema)
    
    end_time = time.perf_counter()
    read_time = end_time - start_time
    
    return table, read_time


def benchmark_parquet_read_performance(parquet_bytes) -> Tuple[pa.Table, float]:
    """Benchmark reading Parquet bytes back to PyArrow Table."""
    start_time = time.perf_counter()
    
    # Handle both bytes and list types (Rust returns list, Python returns bytes)
    if isinstance(parquet_bytes, list):
        parquet_bytes = bytes(parquet_bytes)
    
    buffer = io.BytesIO(parquet_bytes)
    table = pq.read_table(buffer)
    end_time = time.perf_counter()
    read_time = end_time - start_time
    
    return table, read_time


def run_benchmark_suite():
    """Run comprehensive benchmark comparing Rust vs Python for IPC and Parquet."""
    print("=" * 80)
    print("🚀 RUST vs PYTHON (PyArrow) IPC & PARQUET BENCHMARK")
    print("=" * 80)
    
    # Test parameters
    test_cases = [
        (1000, "1K contracts"),
        (10000, "10K contracts"),
        (50000, "50K contracts"),
    ]
    
    # Filtering parameters
    current_date = 20231110  # Nov 10, 2023
    max_dte = 30
    base_pct = 0.10  # 10% base moneyness
    
    print("Test Parameters:")
    print(f"  Current Date: {current_date}")
    print("  Time-matched underlying pricing: Enabled")
    print(f"  Max DTE: {max_dte} days")
    print(f"  Base Moneyness: {base_pct*100}%")
    print()
    
    all_results = []
    
    for num_contracts, description in test_cases:
        print(f"📊 Testing {description}")
        print("-" * 60)
        
        # Generate test data
        print("  Generating test data...")
        option_data, stock_data = generate_realistic_test_data(num_contracts)
        total_size_mb = (len(option_data) + len(stock_data)) / (1024 * 1024)
        print(f"  Total JSON size: {total_size_mb:.2f} MB")
        
        results = {
            'contracts': num_contracts,
            'description': description,
            'json_size_mb': total_size_mb
        }
        
        # ========== IPC BENCHMARKS ==========
        print("\n  🔄 IPC Serialization Benchmarks:")
        
        # Python PyArrow IPC
        print("    Running Python PyArrow IPC...")
        py_ipc_result, py_ipc_time, py_ipc_memory = benchmark_implementation(
            "Python IPC",
            python_filter_to_ipc_bytes,
            option_data, stock_data, current_date, max_dte, base_pct
        )
        py_ipc_size_kb = len(py_ipc_result) / 1024
        py_ipc_ticks = count_ticks_from_ipc_bytes(py_ipc_result)
        
        # Rust IPC
        print("    Running Rust IPC...")
        rust_ipc_result, rust_ipc_time, rust_ipc_memory = benchmark_implementation(
            "Rust IPC",
            fast_parser.filter_contracts_to_ipc_bytes,
            option_data, stock_data, current_date, max_dte, base_pct
        )
        rust_ipc_size_kb = len(rust_ipc_result) / 1024
        rust_ipc_ticks = count_ticks_from_ipc_bytes(rust_ipc_result)
        ipc_speedup = py_ipc_time / rust_ipc_time if rust_ipc_time > 0 else 0
        ipc_size_diff = ((rust_ipc_size_kb - py_ipc_size_kb) / py_ipc_size_kb * 100) if py_ipc_size_kb > 0 else 0
        
        # ========== PARQUET BENCHMARKS ==========
        print("\n  📦 Parquet Serialization Benchmarks:")
        
        # Python PyArrow Parquet
        print("    Running Python PyArrow Parquet...")
        py_pqt_result, py_pqt_time, py_pqt_memory = benchmark_implementation(
            "Python Parquet",
            python_filter_to_parquet_bytes,
            option_data, stock_data, current_date, max_dte, base_pct, None
        )
        py_pqt_size_kb = len(py_pqt_result) / 1024
        py_pqt_ticks = count_ticks_from_parquet_bytes(py_pqt_result)
        
        # Rust Parquet

        print("    Running Rust Parquet...")
        rust_pqt_result, rust_pqt_time, rust_pqt_memory = benchmark_implementation(
            "Rust Parquet",
            fast_parser.filter_contracts_to_parquet_bytes,
            option_data, stock_data, current_date, max_dte, base_pct
        )
        rust_pqt_size_kb = len(rust_pqt_result) / 1024
        rust_pqt_ticks = count_ticks_from_parquet_bytes(rust_pqt_result)
        pqt_speedup = py_pqt_time / rust_pqt_time if rust_pqt_time > 0 else 0
        pqt_size_diff = ((rust_pqt_size_kb - py_pqt_size_kb) / py_pqt_size_kb * 100) if py_pqt_size_kb > 0 else 0

        
        # ========== READ PERFORMANCE BENCHMARKS ==========
        print("\n  📖 Read Performance Benchmarks:")
        
        # Test reading IPC data back
        if len(py_ipc_result) > 0:
            print("    Testing IPC read performance...")
            _, py_ipc_read_time = benchmark_ipc_read_performance(py_ipc_result)
            
            _, rust_ipc_read_time = benchmark_ipc_read_performance(rust_ipc_result)
            ipc_read_ratio = py_ipc_read_time / rust_ipc_read_time if rust_ipc_read_time > 0 else 0
        else:
            py_ipc_read_time = rust_ipc_read_time = ipc_read_ratio = 0
        
        # Test reading Parquet data back  
        if len(py_pqt_result) > 0:
            print("    Testing Parquet read performance...")
            _, py_pqt_read_time = benchmark_parquet_read_performance(py_pqt_result)
            
            if len(rust_pqt_result) > 0:
                _, rust_pqt_read_time = benchmark_parquet_read_performance(rust_pqt_result)
                pqt_read_ratio = py_pqt_read_time / rust_pqt_read_time if rust_pqt_read_time > 0 else 0
        else:
            py_pqt_read_time = rust_pqt_read_time = pqt_read_ratio = 0
        
        # Compare IPC vs Parquet read speeds (using Python results)
        if py_ipc_read_time > 0 and py_pqt_read_time > 0:
            ipc_vs_pqt_read_speed = py_pqt_read_time / py_ipc_read_time
        else:
            ipc_vs_pqt_read_speed = 0
        
        # Store results
        results.update({
            # IPC results
            'py_ipc_time': py_ipc_time,
            'py_ipc_memory': py_ipc_memory,
            'py_ipc_size_kb': py_ipc_size_kb,
            'py_ipc_ticks': py_ipc_ticks,
            'rust_ipc_time': rust_ipc_time,
            'rust_ipc_memory': rust_ipc_memory,
            'rust_ipc_size_kb': rust_ipc_size_kb,
            'rust_ipc_ticks': rust_ipc_ticks,
            'ipc_speedup': ipc_speedup,
            'ipc_size_diff': ipc_size_diff,
            
            # Parquet results
            'py_pqt_time': py_pqt_time,
            'py_pqt_memory': py_pqt_memory,
            'py_pqt_size_kb': py_pqt_size_kb,
            'py_pqt_ticks': py_pqt_ticks,
            'rust_pqt_time': rust_pqt_time,
            'rust_pqt_memory': rust_pqt_memory,
            'rust_pqt_size_kb': rust_pqt_size_kb,
            'rust_pqt_ticks': rust_pqt_ticks,
            'pqt_speedup': pqt_speedup,
            'pqt_size_diff': pqt_size_diff,
            
            # Read performance results
            'py_ipc_read_time': py_ipc_read_time,
            'rust_ipc_read_time': rust_ipc_read_time,
            'py_pqt_read_time': py_pqt_read_time,
            'rust_pqt_read_time': rust_pqt_read_time,
            'ipc_read_ratio': ipc_read_ratio,
            'pqt_read_ratio': pqt_read_ratio,
            'ipc_vs_pqt_read_speed': ipc_vs_pqt_read_speed,
        })
        
        all_results.append(results)
        
        # Print individual results
        print("\n  📊 Results Summary:")
        print(f"    Filtered ticks: {py_ipc_ticks}")
        print("    \n    IPC Comparison:")
        print(f"      Python: {py_ipc_time*1000:.1f}ms, {py_ipc_memory:.1f}MB, {py_ipc_size_kb:.1f}KB")
        print(f"      Rust:   {rust_ipc_time*1000:.1f}ms, {rust_ipc_memory:.1f}MB, {rust_ipc_size_kb:.1f}KB")
        print(f"      Speedup: {ipc_speedup:.1f}x, Size diff: {ipc_size_diff:+.1f}%")
    
        print("    \n    Parquet Comparison:")
        print(f"      Python: {py_pqt_time*1000:.1f}ms, {py_pqt_memory:.1f}MB, {py_pqt_size_kb:.1f}KB")
        print(f"      Rust:   {rust_pqt_time*1000:.1f}ms, {rust_pqt_memory:.1f}MB, {rust_pqt_size_kb:.1f}KB")
        print(f"      Speedup: {pqt_speedup:.1f}x, Size diff: {pqt_size_diff:+.1f}%")
        
        print("    \n    Read Performance:")
        print(f"      IPC read:     {py_ipc_read_time*1000:.1f}ms")
        print(f"      Parquet read: {py_pqt_read_time*1000:.1f}ms")
        if py_ipc_read_time > 0 and py_pqt_read_time > 0:
            if py_ipc_read_time < py_pqt_read_time:
                faster_format = "IPC"
                speed_ratio = py_pqt_read_time / py_ipc_read_time
            else:
                faster_format = "Parquet"
                speed_ratio = py_ipc_read_time / py_pqt_read_time
            print(f"      {faster_format} is {speed_ratio:.1f}x faster to read")
        
        print()
    
    # Print summary table
    print("=" * 100)
    print("📈 COMPREHENSIVE BENCHMARK SUMMARY")
    print("=" * 100)
    
    print("\n🔄 IPC Serialization Performance:")
    print("-" * 80)
    header = f"{'Dataset':<12} {'Python':<12} {'Rust':<12} {'Speedup':<10} {'Py Size':<10} {'Rust Size':<10}"
    print(header)
    print("-" * len(header))
    
    for r in all_results:
        row = (f"{r['description']:<12} "
                f"{r['py_ipc_time']*1000:>8.1f}ms "
                f"{r['rust_ipc_time']*1000:>8.1f}ms "
                f"{r['ipc_speedup']:>7.1f}x "
                f"{r['py_ipc_size_kb']:>7.1f}KB "
                f"{r['rust_ipc_size_kb']:>8.1f}KB")
        print(row)
        
    print("\n📦 Parquet Serialization Performance:")
    print("-" * 80)
    header = f"{'Dataset':<12} {'Python':<12} {'Rust':<12} {'Speedup':<10} {'Py Size':<10} {'Rust Size':<10}"
    print(header)
    print("-" * len(header))
    
    for r in all_results:
        row = (f"{r['description']:<12} "
                f"{r['py_pqt_time']*1000:>8.1f}ms "
                f"{r['rust_pqt_time']*1000:>8.1f}ms "
                f"{r['pqt_speedup']:>7.1f}x "
                f"{r['py_pqt_size_kb']:>7.1f}KB "
                f"{r['rust_pqt_size_kb']:>8.1f}KB")
        print(row)
    
    print("\n📖 Read Performance Comparison:")
    print("-" * 80)
    header = f"{'Dataset':<12} {'IPC Read':<12} {'Parquet Read':<15} {'Faster Format':<15} {'Speed Ratio':<12}"
    print(header)
    print("-" * len(header))
    
    for r in all_results:
        ipc_read_ms = r['py_ipc_read_time'] * 1000
        pqt_read_ms = r['py_pqt_read_time'] * 1000
        
        if ipc_read_ms > 0 and pqt_read_ms > 0:
            if ipc_read_ms < pqt_read_ms:
                faster = "IPC"
                ratio = pqt_read_ms / ipc_read_ms
            else:
                faster = "Parquet"
                ratio = ipc_read_ms / pqt_read_ms
        else:
            faster = "N/A"
            ratio = 0
            
        row = (f"{r['description']:<12} "
                f"{ipc_read_ms:>8.1f}ms "
                f"{pqt_read_ms:>11.1f}ms "
                f"{faster:<15} "
                f"{ratio:>8.1f}x")
        print(row)
    
    # Calculate averages
    avg_ipc_speedup = sum(r['ipc_speedup'] for r in all_results) / len(all_results)
    avg_pqt_speedup = sum(r['pqt_speedup'] for r in all_results) / len(all_results)
    
    print("\n🎯 KEY FINDINGS:")
    print(f"  Average IPC speedup: {avg_ipc_speedup:.1f}x faster")
    print(f"  Average Parquet speedup: {avg_pqt_speedup:.1f}x faster")
    
    # Compression comparison
    largest_test = max(all_results, key=lambda x: x['contracts'])
    ipc_vs_pqt_python = (largest_test['py_pqt_size_kb'] / largest_test['py_ipc_size_kb'] * 100) if largest_test['py_ipc_size_kb'] > 0 else 0
    ipc_vs_pqt_rust = (largest_test['rust_pqt_size_kb'] / largest_test['rust_ipc_size_kb'] * 100) if largest_test['rust_ipc_size_kb'] > 0 else 0
    
    print(f"  Parquet vs IPC size (Python): {ipc_vs_pqt_python:.1f}% of IPC size")
    print(f"  Parquet vs IPC size (Rust): {ipc_vs_pqt_rust:.1f}% of IPC size")
    
    # Read performance analysis
    avg_ipc_read_time = sum(r['py_ipc_read_time'] for r in all_results) / len(all_results) * 1000
    avg_pqt_read_time = sum(r['py_pqt_read_time'] for r in all_results) / len(all_results) * 1000
    
    if avg_ipc_read_time > 0 and avg_pqt_read_time > 0:
        if avg_ipc_read_time < avg_pqt_read_time:
            read_winner = "IPC"
            read_advantage = avg_pqt_read_time / avg_ipc_read_time
        else:
            read_winner = "Parquet"
            read_advantage = avg_ipc_read_time / avg_pqt_read_time
        print(f"  Average read performance: {read_winner} is {read_advantage:.1f}x faster to read")
    
    # Throughput
    rust_ipc_throughput = largest_test['rust_ipc_ticks'] / largest_test['rust_ipc_time'] if largest_test['rust_ipc_time'] > 0 else None
    rust_pqt_throughput = largest_test['rust_pqt_ticks'] / largest_test['rust_pqt_time'] if largest_test['rust_pqt_time'] > 0 else None
    
    print(f"  Rust IPC throughput: {rust_ipc_throughput:,.0f} ticks/second")
    print(f"  Rust Parquet throughput: {rust_pqt_throughput:,.0f} ticks/second")


if __name__ == "__main__":
    run_benchmark_suite()
    #test = generate_realistic_test_data(1)
    #print(test)