use chrono::{NaiveDate, Datelike};
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, Float32Array, UInt32Array, StringArray, BooleanArray, BooleanBuilder};
use arrow::compute::{filter_record_batch, and};
use std::collections::HashMap;

/// Fast square root lookup table for common DTE values
use std::sync::LazyLock;
static SQRT_LUT: LazyLock<[f32; 366]> = LazyLock::new(|| {
    let mut lut = [0.0; 366];
    for i in 0..366 {
        lut[i] = (i as f32).sqrt();
    }
    lut
});

/// Fast square root using lookup table for small values
#[inline(always)]
fn fast_sqrt(dte: i64) -> f32 {
    if dte < 366 {
        SQRT_LUT[dte as usize]
    } else {
        (dte as f32).sqrt()
    }
}

/// Ultra-fast date parsing with branch-free validation
#[inline(always)]
fn parse_yyyymmdd_branchless(date: u32) -> Option<NaiveDate> {
    let year = (date / 10000) as i32;
    let month = ((date % 10000) / 100) as u32;
    let day = (date % 100) as u32;
    
    // Branch-free validation using bit tricks
    let month_valid = (month.wrapping_sub(1) < 12) as u32;
    let day_valid = (day.wrapping_sub(1) < 31) as u32;
    
    if month_valid & day_valid != 0 {
        NaiveDate::from_ymd_opt(year, month, day)
    } else {
        None
    }
}


/// Create DTE (Days to Expiration) filter mask
pub fn filter_dte_arrow(
    expiration_array: &UInt32Array,
    current_date: NaiveDate,
    max_dte: i64,
) -> BooleanArray {
    let mut builder = BooleanBuilder::with_capacity(expiration_array.len());
    
    for i in 0..expiration_array.len() {
        let expiration = expiration_array.value(i);
        
        if let Some(exp_date) = parse_yyyymmdd_branchless(expiration) {
            let dte = (exp_date - current_date).num_days();
            builder.append_value(dte > 0 && dte <= max_dte);
        } else {
            builder.append_value(false);
        }
    }
    
    builder.finish()
}

/// Create dynamic threshold array based on DTE
pub fn compute_dynamic_thresholds(
    expiration_array: &UInt32Array,
    current_date: NaiveDate,
    base_pct: f64,
) -> Float32Array {
    let mut builder = arrow::array::Float32Builder::with_capacity(expiration_array.len());
    
    for i in 0..expiration_array.len() {
        let expiration = expiration_array.value(i);
        
        if let Some(exp_date) = parse_yyyymmdd_branchless(expiration) {
            let dte = (exp_date - current_date).num_days();
            
            if dte > 0 {
                let threshold = base_pct as f32 * fast_sqrt(dte);
                builder.append_value(threshold);
            } else {
                builder.append_value(0.0);
            }
        } else {
            builder.append_value(0.0);
        }
    }
    
    builder.finish()
}

/// Main Arrow-native filtering function with time-matched underlying prices
pub fn filter_contracts_arrow_native(
    batch: RecordBatch,
    current_date: NaiveDate,
    price_index: HashMap<u32, f32>,
    max_dte: i64,
    base_pct: f64,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    // Validate inputs
    if batch.num_rows() == 0 {
        return Ok(batch); // Return empty batch as-is
    }
    
    if price_index.is_empty() {
        return Err("Price index cannot be empty".into());
    }
    
    if max_dte <= 0 {
        return Err("Max DTE must be positive".into());
    }
    
    if base_pct <= 0.0 || base_pct > 1.0 {
        return Err("Base percentage must be between 0 and 1".into());
    }
    
    // Extract arrays from RecordBatch with bounds checking
    let expiration_array = batch
        .column_by_name("expiration")
        .ok_or("Missing expiration column")?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or("Invalid expiration column type")?;
    
    let strike_array = batch
        .column_by_name("strike")
        .ok_or("Missing strike column")?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or("Invalid strike column type")?;
    
    let ms_of_day_array = batch
        .column_by_name("ms_of_day")
        .ok_or("Missing ms_of_day column")?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or("Invalid ms_of_day column type")?;
    
    let right_array = batch
        .column_by_name("right")
        .ok_or("Missing right column")?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("Invalid right column type")?;
    
    // Validate array lengths match
    if expiration_array.len() != strike_array.len() || 
       expiration_array.len() != ms_of_day_array.len() ||
       expiration_array.len() != right_array.len() {
        return Err("Array lengths are mismatched".into());
    }
    
    if expiration_array.len() != batch.num_rows() {
        return Err("Array length does not match batch row count".into());
    }
    
    // Convert strikes to dollars (Float32Array)
    let strike_dollars = {
        let mut builder = arrow::array::Float32Builder::with_capacity(strike_array.len());
        for i in 0..strike_array.len() {
            let strike_minor = strike_array.value(i);
            builder.append_value((strike_minor as f64 * 0.0001) as f32);
        }
        builder.finish()
    };
    
    // 1. Filter by DTE using fast date parsing
    let dte_mask = filter_dte_arrow(expiration_array, current_date, max_dte);
    
    // 2. Compute dynamic thresholds based on DTE
    let thresholds = compute_dynamic_thresholds(expiration_array, current_date, base_pct);
    
    // Validate all arrays have consistent lengths
    if dte_mask.len() != expiration_array.len() || 
       thresholds.len() != expiration_array.len() ||
       strike_dollars.len() != expiration_array.len() {
        return Err("Computed arrays have inconsistent lengths".into());
    }
    
    // 3. Safe moneyness filtering with time-matched underlying prices
    // Validate array lengths match to prevent bounds errors
    if strike_dollars.len() != thresholds.len() {
        return Err("Strike and threshold arrays have mismatched lengths".into());
    }
    
    let mut moneyness_builder = BooleanBuilder::with_capacity(strike_dollars.len());
    
    // Use safe scalar implementation with time-matched underlying prices
    for i in 0..strike_dollars.len() {
        let strike = strike_dollars.value(i);
        let threshold = thresholds.value(i);
        let ms_of_day = ms_of_day_array.value(i);
        let right = right_array.value(i);
        
        // Skip underlying stock records (identified by right = "U")
        if right == "U" {
            moneyness_builder.append_value(false);
            continue;
        }
        
        // Get time-matched underlying price
        let underlying = match price_index.get(&ms_of_day) {
            Some(&price) => price,
            None => {
                // No exact time match - use closest available price
                // For now, skip this record if no exact match
                moneyness_builder.append_value(false);
                continue;
            }
        };
        
        let diff = (strike - underlying).abs();
        let within_threshold = diff <= underlying * threshold;
        moneyness_builder.append_value(within_threshold);
    }
    
    let moneyness_mask = moneyness_builder.finish();
    
    // 4. Combine masks using Arrow compute kernels
    let final_mask = and(&dte_mask, &moneyness_mask)?;
    
    // 5. Apply filter to RecordBatch
    let filtered_batch = filter_record_batch(&batch, &final_mask)?;
    
    Ok(filtered_batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_fast_sqrt() {
        for i in 1..366 {
            let expected = (i as f32).sqrt();
            let actual = fast_sqrt(i);
            assert!((expected - actual).abs() < 0.001, "sqrt({}) mismatch", i);
        }
    }
    
    #[test]
    fn test_dynamic_threshold_computation() {
        use arrow::array::UInt32Array;
        use chrono::NaiveDate;
        
        let expirations = UInt32Array::from(vec![20240101, 20240115, 20240201]);
        let current_date = NaiveDate::from_ymd_opt(2023, 12, 1).unwrap();
        let base_pct = 0.1;
        
        let thresholds = compute_dynamic_thresholds(&expirations, current_date, base_pct);
        
        // Verify threshold increases with DTE (sqrt relationship)
        assert!(thresholds.value(0) > 0.0);
        assert!(thresholds.value(1) > thresholds.value(0)); // Longer DTE = higher threshold
        assert!(thresholds.value(2) > thresholds.value(1)); // Even longer DTE = highest threshold
    }
}