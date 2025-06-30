use crate::types::{JsonResponse, JsonStockResponse, TickBatchBuilder, JsonContract};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

/// Parse both option and stock JSON to combined Arrow RecordBatch with price index
#[cfg(feature = "simd")]
pub fn parse_jsons_to_combined_arrow(
    option_json: &str,
    stock_json: &str,
) -> Result<(RecordBatch, HashMap<u32, f32>), Box<dyn std::error::Error + Send + Sync>> {
    // Parse option JSON with SIMD acceleration
    let mut option_bytes = option_json.as_bytes().to_vec();
    let option_response: JsonResponse = simd_json::from_slice(&mut option_bytes)?;
    
    // Parse stock JSON with SIMD acceleration
    let mut stock_bytes = stock_json.as_bytes().to_vec();
    let stock_response: JsonStockResponse = simd_json::from_slice(&mut stock_bytes)?;
    
    parse_combined_data(option_response, stock_response)
}

/// Fallback parser for when SIMD is not available
#[cfg(not(feature = "simd"))]
pub fn parse_jsons_to_combined_arrow(
    option_json: &str,
    stock_json: &str,
) -> Result<(RecordBatch, HashMap<u32, f32>), Box<dyn std::error::Error + Send + Sync>> {
    // Parse option JSON
    let option_response: JsonResponse = serde_json::from_str(option_json)?;
    
    // Parse stock JSON
    let stock_response: JsonStockResponse = serde_json::from_str(stock_json)?;
    
    parse_combined_data(option_response, stock_response)
}

/// Common logic for parsing combined option and stock data
fn parse_combined_data(
    option_response: JsonResponse,
    stock_response: JsonStockResponse,
) -> Result<(RecordBatch, HashMap<u32, f32>), Box<dyn std::error::Error + Send + Sync>> {
    // Calculate total tick capacity
    let option_ticks: usize = option_response.response.iter()
        .map(|entry| entry.ticks.len())
        .sum();
    let stock_ticks = stock_response.response.len();
    let total_ticks = option_ticks + stock_ticks;
    
    if total_ticks == 0 {
        return Ok((TickBatchBuilder::new(0).finish(), HashMap::new()));
    }
    
    // Build price index from stock data
    let mut price_index = HashMap::with_capacity(stock_ticks);
    for stock_tick in &stock_response.response {
        let ms_of_day = stock_tick.0;
        let bid = stock_tick.3 as f32;
        let ask = stock_tick.7 as f32;
        let midpoint = (bid + ask) / 2.0;
        price_index.insert(ms_of_day, midpoint);
    }
    
    // Create Arrow builder with exact capacity
    let mut builder = TickBatchBuilder::new(total_ticks);
    
    // Add option ticks (keep original structure)
    for entry in &option_response.response {
        let contract = &entry.contract;
        for tick in &entry.ticks {
            builder.append_tick(tick, contract);
        }
    }
    
    // Add stock ticks as underlying records
    let underlying_contract = JsonContract {
        root: "STOCK".to_string(), // Will be overridden by actual root
        expiration: 0,  // Marker for underlying
        strike: 0,      // No strike for underlying
        right: "U".to_string(), // Underlying marker
    };
    
    for stock_tick in &stock_response.response {
        builder.append_tick(stock_tick, &underlying_contract);
    }
    
    Ok((builder.finish(), price_index))
}

