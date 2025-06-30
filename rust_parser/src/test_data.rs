#[cfg(test)]
pub mod test_data {
    /// Generate test JSON response for testing
    #[allow(dead_code)]
    pub fn sample_bulk_response() -> String {
        r#"{
            "header": {
                "latency_ms": 100,
                "format": ["ms_of_day","bid_size","bid_exchange","bid","bid_condition","ask_size","ask_exchange","ask","ask_condition","date"]
            },
            "response": [
                {
                    "ticks": [
                        [34200000, 10, 47, 149.50, 50, 10, 47, 150.00, 50, 20231117],
                        [34260000, 15, 47, 149.75, 50, 12, 47, 150.25, 50, 20231117]
                    ],
                    "contract": {
                        "root": "AAPL",
                        "expiration": 20231117,
                        "strike": 1500000,
                        "right": "C"
                    }
                },
                {
                    "ticks": [
                        [34200000, 8, 47, 164.50, 50, 8, 47, 165.00, 50, 20231117]
                    ],
                    "contract": {
                        "root": "AAPL",
                        "expiration": 20231117,
                        "strike": 1650000,
                        "right": "C"
                    }
                },
                {
                    "ticks": [
                        [34200000, 5, 47, 179.00, 50, 5, 47, 180.00, 50, 20231117]
                    ],
                    "contract": {
                        "root": "AAPL",
                        "expiration": 20231117,
                        "strike": 1800000,
                        "right": "C"
                    }
                },
                {
                    "ticks": [
                        [34200000, 12, 47, 149.25, 50, 10, 47, 149.75, 50, 20231124]
                    ],
                    "contract": {
                        "root": "AAPL",
                        "expiration": 20231124,
                        "strike": 1500000,
                        "right": "C"
                    }
                },
                {
                    "ticks": [
                        [34200000, 6, 47, 134.00, 50, 8, 47, 134.50, 50, 20231117]
                    ],
                    "contract": {
                        "root": "AAPL",
                        "expiration": 20231117,
                        "strike": 1350000,
                        "right": "P"
                    }
                },
                {
                    "ticks": [
                        [34200000, 4, 47, 119.00, 50, 6, 47, 120.00, 50, 20231117]
                    ],
                    "contract": {
                        "root": "AAPL",
                        "expiration": 20231117,
                        "strike": 1200000,
                        "right": "P"
                    }
                }
            ]
        }"#.to_string()
    }
    
    /// Generate large test dataset for performance testing
    #[allow(dead_code)]
    pub fn large_test_response(num_contracts: usize) -> String {
        let mut contracts = Vec::new();
        
        for i in 0..num_contracts {
            let strike = 1400000 + (i * 5000) as u32; // $140-200 range
            let expiration = 20231117 + (i % 14) as u32; // Vary expirations
            let right = if i % 2 == 0 { "C" } else { "P" };
            
            let contract_json = format!(
                r#"{{
                    "ticks": [
                        [34200000, 10, 47, {:.2}, 50, 10, 47, {:.2}, 50, {}]
                    ],
                    "contract": {{
                        "root": "AAPL",
                        "expiration": {},
                        "strike": {},
                        "right": "{}"
                    }}
                }}"#,
                strike as f64 / 10000.0 - 0.25, // bid
                strike as f64 / 10000.0 + 0.25, // ask
                expiration,
                expiration,
                strike,
                right
            );
            
            contracts.push(contract_json);
        }
        
        format!(
            r#"{{
                "header": {{
                    "latency_ms": 50,
                    "format": ["ms_of_day","bid_size","bid_exchange","bid","bid_condition","ask_size","ask_exchange","ask","ask_condition","date"]
                }},
                "response": [{}]
            }}"#,
            contracts.join(",")
        )
    }
}