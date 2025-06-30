use pyo3::prelude::*;
use chrono::NaiveDate;
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::StreamWriter;
use std::io::Cursor;
use parquet::file::properties::WriterProperties;

// Core modules
mod types;
mod parser;
mod filter;

#[cfg(test)]
mod test_data;

use crate::parser::parse_jsons_to_combined_arrow;
use crate::filter::filter_contracts_arrow_native;

/// Arrow-native contract filtering with IPC serialization
/// 
/// Returns Arrow IPC bytes for direct streaming to Kafka or other systems
#[pyfunction]
fn filter_contracts_to_ipc_bytes(
    option_json: &str,
    stock_json: &str,
    current_yyyymmdd: u32,
    max_dte: i64,
    base_pct: f64,
) -> PyResult<Vec<u8>> {
    
    let current_date = parse_current_date_fast(current_yyyymmdd)?;
    
    // Parse both JSONs and get combined Arrow RecordBatch with price index
    let (batch, price_index) = parse_jsons_to_combined_arrow(option_json, stock_json)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(
            format!("JSON parsing failed: {}", e)
        ))?;
    
    if batch.num_rows() == 0 {
        return serialize_empty_batch_to_ipc();
    }
    
    // Apply Arrow filtering using time-matched underlying prices
    let filtered_batch = filter_contracts_arrow_native(
        batch,
        current_date,
        price_index,
        max_dte,
        base_pct,
    ).map_err(|e| pyo3::exceptions::PyValueError::new_err(
        format!("Filtering failed: {}", e)
    ))?;
    
    // Serialize to Arrow IPC bytes
    serialize_batch_to_ipc(&filtered_batch)
}

/// Arrow-native contract filtering with Parquet serialization
/// 
/// Returns Parquet bytes for efficient storage and analytics
#[pyfunction]
fn filter_contracts_to_parquet_bytes(
    option_json: &str,
    stock_json: &str,
    current_yyyymmdd: u32,
    max_dte: i64,
    base_pct: f64
) -> PyResult<Vec<u8>> {
    
    let current_date = parse_current_date_fast(current_yyyymmdd)?;
    
    // Parse both JSONs and get combined Arrow RecordBatch with price index
    let (batch, price_index) = parse_jsons_to_combined_arrow(option_json, stock_json)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(
            format!("JSON parsing failed: {}", e)
        ))?;
    
    if batch.num_rows() == 0 {
        return serialize_empty_batch_to_parquet();
    }
    
    // Apply Arrow filtering using time-matched underlying prices
    let filtered_batch = filter_contracts_arrow_native(
        batch,
        current_date,
        price_index,
        max_dte,
        base_pct,
    ).map_err(|e| pyo3::exceptions::PyValueError::new_err(
        format!("Filtering failed: {}", e)
    ))?;
    
    // Serialize to Parquet bytes
    serialize_batch_to_parquet(&filtered_batch)
}




/// Fast date parsing with optimized validation
#[inline]
fn parse_current_date_fast(date: u32) -> PyResult<NaiveDate> {
    let year = (date / 10000) as i32;
    let month = ((date % 10000) / 100) as u32;
    let day = (date % 100) as u32;
    
    // Fast validation
    if month == 0 || month > 12 || day == 0 || day > 31 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            format!("Invalid date format: {}", date)
        ));
    }
    
    NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(
            format!("Invalid date: {}", date)
        ))
}

/// Serialize RecordBatch to Arrow IPC bytes with safety checks
fn serialize_batch_to_ipc(batch: &RecordBatch) -> PyResult<Vec<u8>> {
    // Validate batch before serialization
    if batch.schema().fields().is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Cannot serialize batch with empty schema"
        ));
    }
    
    // Pre-allocate buffer with reasonable size to avoid repeated allocations
    let estimated_size = batch.num_rows() * batch.num_columns() * 8; // rough estimate
    let mut buffer = Vec::with_capacity(estimated_size.min(1024 * 1024)); // cap at 1MB
    
    {
        let mut cursor = Cursor::new(&mut buffer);
        let mut writer = StreamWriter::try_new(&mut cursor, &batch.schema())
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                format!("Failed to create StreamWriter: {}", e)
            ))?;
        
        writer.write(batch)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                format!("Failed to write RecordBatch: {}", e)
            ))?;
        
        writer.finish()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                format!("Failed to finish stream: {}", e)
            ))?;
    }
    
    // Validate output size is reasonable
    if buffer.len() > 100 * 1024 * 1024 { // 100MB limit
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Serialized data exceeds size limit"
        ));
    }
    
    Ok(buffer)
}

/// Create empty Arrow IPC bytes with proper schema
fn serialize_empty_batch_to_ipc() -> PyResult<Vec<u8>> {
    use crate::types::TickBatchBuilder;
    let empty_batch = TickBatchBuilder::new(0).finish();
    serialize_batch_to_ipc(&empty_batch)
}

/// Serialize RecordBatch to Parquet bytes with safety checks
fn serialize_batch_to_parquet(batch: &RecordBatch) -> PyResult<Vec<u8>> {
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    
    // Validate batch before serialization
    if batch.schema().fields().is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Cannot serialize batch with empty schema"
        ));
    }
    
    // Pre-allocate buffer with reasonable size
    let estimated_size = batch.num_rows() * batch.num_columns() * 4; // Parquet is compressed
    let mut buffer = Vec::with_capacity(estimated_size.min(512 * 1024)); // cap at 512KB
    
    {
        let cursor = Cursor::new(&mut buffer);
        
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build();
        
        let mut writer = ArrowWriter::try_new(cursor, batch.schema(), Some(props))
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                format!("Failed to create Parquet writer: {}", e)
            ))?;
        
        writer.write(batch)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                format!("Failed to write Parquet batch: {}", e)
            ))?;
        
        writer.close()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                format!("Failed to close Parquet writer: {}", e)
            ))?;
    }
    
    // Validate output size is reasonable
    if buffer.len() > 50 * 1024 * 1024 { // 50MB limit for Parquet
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Serialized Parquet data exceeds size limit"
        ));
    }
    
    Ok(buffer)
}

/// Create empty Parquet bytes with proper schema
fn serialize_empty_batch_to_parquet() -> PyResult<Vec<u8>> {
    use crate::types::TickBatchBuilder;
    let empty_batch = TickBatchBuilder::new(0).finish();
    serialize_batch_to_parquet(&empty_batch)
}



/// Module definition for Python import
#[pymodule]
fn fast_parser(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(filter_contracts_to_ipc_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(filter_contracts_to_parquet_bytes, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_test_option_response() -> String {
        r#"{
            "header": {
                "latency_ms": 100,
                "format": ["ms_of_day","bid_size","bid_exchange","bid","bid_condition","ask_size","ask_exchange","ask","ask_condition","date"]
            },
            "response": [
                {
                    "ticks": [
                        [34200000, 10, 47, 149.50, 50, 10, 47, 150.00, 50, 20231117],
                        [35100000, 13, 47, 148.75, 50, 11, 47, 149.25, 50, 20231117]
                    ],
                    "contract": {"root": "AAPL", "expiration": 20231117, "strike": 1500000, "right": "C"}
                },
                {
                    "ticks": [
                        [34200000, 8, 47, 164.50, 50, 8, 47, 165.00, 50, 20231117]
                    ],
                    "contract": {"root": "AAPL", "expiration": 20231117, "strike": 1650000, "right": "C"}
                }
            ]
        }"#.to_string()
    }
    
    fn create_test_stock_response() -> String {
        r#"{
            "header": {
                "latency_ms": 50,
                "format": ["ms_of_day","bid_size","bid_exchange","bid","bid_condition","ask_size","ask_exchange","ask","ask_condition","date"]
            },
            "response": [
                [34200000, 100, 47, 149.75, 50, 100, 47, 149.85, 50, 20231117],
                [35100000, 150, 47, 148.95, 50, 120, 47, 149.05, 50, 20231117]
            ]
        }"#.to_string()
    }
    
    #[test]
    fn test_combined_arrow_parsing() {
        let option_json = create_test_option_response();
        let stock_json = create_test_stock_response();
        
        let (batch, price_index) = parse_jsons_to_combined_arrow(&option_json, &stock_json).unwrap();
        
        // Should have 5 ticks total (3 option + 2 stock)
        assert_eq!(batch.num_rows(), 5);
        
        // Should have 2 prices in the index
        assert_eq!(price_index.len(), 2);
        
        // Check schema has all expected columns
        let schema = batch.schema();
        assert!(schema.column_with_name("ms_of_day").is_some());
        assert!(schema.column_with_name("bid_price").is_some());
        assert!(schema.column_with_name("ask_price").is_some());
        assert!(schema.column_with_name("root").is_some());
        assert!(schema.column_with_name("expiration").is_some());
        assert!(schema.column_with_name("strike").is_some());
        assert!(schema.column_with_name("right").is_some());
    }
    
    #[test]
    fn test_arrow_ipc_filtering() {
        let option_json = create_test_option_response();
        let stock_json = create_test_stock_response();
        
        let result = filter_contracts_to_ipc_bytes(
            &option_json,
            &stock_json,
            20231110, // Current date
            30,       // Max DTE
            0.1,      // 10% base percentage
        );
        
        assert!(result.is_ok());
        let ipc_bytes = result.unwrap();
        assert!(!ipc_bytes.is_empty());
        
        // Arrow IPC stream should have proper header and data
        assert!(ipc_bytes.len() > 200); // Should contain schema + data
    }
    
    #[test]
    fn test_parquet_filtering() {
        let option_json = create_test_option_response();
        let stock_json = create_test_stock_response();
        
        let result = filter_contracts_to_parquet_bytes(
            &option_json,
            &stock_json,
            20231110, // Current date
            30,       // Max DTE
            0.1,      // 10% base percentage
        );
        
        assert!(result.is_ok());
        let parquet_bytes = result.unwrap();
        assert!(!parquet_bytes.is_empty());
        
        // Parquet file should have proper header and compressed data
        assert!(parquet_bytes.len() > 100); // Should contain Parquet header + data
    }
}