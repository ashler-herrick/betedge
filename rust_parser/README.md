## Build Instructions

### Development Build
```bash
cargo build --release --features fast
```

### Production Build (Maximum Optimization)
```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release --features fast
```

### Python Extension
```bash
# Install maturin for Python extension building
pip install maturin

# Build Python wheel
maturin build --release --features fast

# Install for development
maturin develop --release --features fast
```

## Features

- `simd`: Enable SIMD-JSON for faster parsing
- `parallel`: Enable Rayon for parallel processing
- `fast`: Enable both SIMD and parallel processing

## Architecture

### Module Structure
```
src/
├── lib.rs              # PyO3 bindings & public API
├── types.rs            # Zero-allocation data structures
├── parser.rs           # SIMD-optimized JSON parsing
└── filter.rs           # Ultra-fast filtering algorithms
```

### Data Flow
1. **Parse JSON** → `Response` struct (SIMD-accelerated)
2. **Extract Contracts** → `Vec<Entry>` (zero-copy where possible)
3. **Filter by DTE** → Remove expired/too-distant contracts
4. **Filter by Moneyness** → Dynamic threshold based on sqrt(DTE)
5. **Convert to Python** → Minimal dictionary structure

## Filtering Algorithm

### Moneyness Calculation
For a given DTE and base percentage (e.g., 10%):
```
threshold = base_pct * sqrt(dte)
include_contract = |strike - underlying| <= underlying * threshold
```

### Examples
- **1 DTE:** 10% * sqrt(1) = 10% threshold
- **4 DTE:** 10% * sqrt(4) = 20% threshold  
- **9 DTE:** 10% * sqrt(9) = 30% threshold
- **30 DTE:** 10% * sqrt(30) ≈ 55% threshold

### Performance Characteristics
- **Memory Usage:** ~28 bytes per tick (PyArrow-level efficiency)
- **CPU Usage:** Vectorized operations where possible
- **Threading:** Optional parallel processing for large datasets
- **Cache Efficiency:** Sequential memory access patterns

## Testing

### Unit Tests
```bash
cargo test --release
```

### Performance Benchmarks
```bash
cargo test --release test_large_dataset_performance -- --nocapture
```
