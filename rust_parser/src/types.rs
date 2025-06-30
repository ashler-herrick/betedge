use serde::Deserialize;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::array::{
    ArrayRef,
    Float32Builder, StringBuilder, UInt32Builder, UInt16Builder, UInt8Builder,
};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// JSON deserialization types - minimal intermediate representation
#[derive(Deserialize)]
pub struct JsonTick(
    pub u32, pub u32, pub u32, pub f64, pub u32, 
    pub u32, pub u32, pub f64, pub u32, pub u32,
);

#[derive(Deserialize)]
pub struct JsonContract {
    pub root: String,
    pub expiration: u32,
    pub strike: u32,
    pub right: String,
}

#[derive(Deserialize)]
pub struct JsonEntry {
    pub ticks: Vec<JsonTick>,
    pub contract: JsonContract,
}

/// Stock quote tick - same format as option tick but simpler context
#[derive(Deserialize)]
pub struct JsonStockTick(
    pub u32, pub u32, pub u32, pub f64, pub u32,
    pub u32, pub u32, pub f64, pub u32, pub u32,
);

/// Trait for tick data that can be appended to Arrow batch
pub trait TickData {
    fn ms_of_day(&self) -> u32;
    fn bid_size(&self) -> u32;
    fn bid_exchange(&self) -> u32;
    fn bid_price(&self) -> f64;
    fn bid_condition(&self) -> u32;
    fn ask_size(&self) -> u32;
    fn ask_exchange(&self) -> u32;
    fn ask_price(&self) -> f64;
    fn ask_condition(&self) -> u32;
    fn date(&self) -> u32;
}

impl TickData for JsonTick {
    fn ms_of_day(&self) -> u32 { self.0 }
    fn bid_size(&self) -> u32 { self.1 }
    fn bid_exchange(&self) -> u32 { self.2 }
    fn bid_price(&self) -> f64 { self.3 }
    fn bid_condition(&self) -> u32 { self.4 }
    fn ask_size(&self) -> u32 { self.5 }
    fn ask_exchange(&self) -> u32 { self.6 }
    fn ask_price(&self) -> f64 { self.7 }
    fn ask_condition(&self) -> u32 { self.8 }
    fn date(&self) -> u32 { self.9 }
}

impl TickData for JsonStockTick {
    fn ms_of_day(&self) -> u32 { self.0 }
    fn bid_size(&self) -> u32 { self.1 }
    fn bid_exchange(&self) -> u32 { self.2 }
    fn bid_price(&self) -> f64 { self.3 }
    fn bid_condition(&self) -> u32 { self.4 }
    fn ask_size(&self) -> u32 { self.5 }
    fn ask_exchange(&self) -> u32 { self.6 }
    fn ask_price(&self) -> f64 { self.7 }
    fn ask_condition(&self) -> u32 { self.8 }
    fn date(&self) -> u32 { self.9 }
}

/// Stock response structure
#[derive(Deserialize)]
pub struct JsonStockResponse {
    #[allow(dead_code)]
    pub header: JsonHeader,
    pub response: Vec<JsonStockTick>,
}

#[derive(Deserialize)]
pub struct JsonHeader {
    #[allow(dead_code)]
    pub latency_ms: u32,
    #[serde(default)]
    #[allow(dead_code)]
    pub format: Vec<String>,
}

#[derive(Deserialize)]
pub struct JsonResponse {
    #[allow(dead_code)]
    pub header: JsonHeader,
    pub response: Vec<JsonEntry>,
}

/// Arrow schema for tick-by-tick option data
pub fn create_tick_schema() -> Schema {
    Schema::new(vec![
        // Tick data fields
        Field::new("ms_of_day", DataType::UInt32, false),
        Field::new("bid_size", DataType::UInt16, false),
        Field::new("bid_exchange", DataType::UInt8, false),
        Field::new("bid_price", DataType::Float32, false),
        Field::new("bid_condition", DataType::UInt8, false),
        Field::new("ask_size", DataType::UInt16, false),
        Field::new("ask_exchange", DataType::UInt8, false),
        Field::new("ask_price", DataType::Float32, false),
        Field::new("ask_condition", DataType::UInt8, false),
        Field::new("date", DataType::UInt32, false),
        
        // Contract fields (repeated for each tick)
        Field::new("root", DataType::Utf8, false),
        Field::new("expiration", DataType::UInt32, false),
        Field::new("strike", DataType::UInt32, false),
        Field::new("right", DataType::Utf8, false),
    ])
}

/// Arrow-native tick batch builder
pub struct TickBatchBuilder {
    // Tick data builders
    ms_of_day: UInt32Builder,
    bid_size: UInt16Builder,
    bid_exchange: UInt8Builder,
    bid_price: Float32Builder,
    bid_condition: UInt8Builder,
    ask_size: UInt16Builder,
    ask_exchange: UInt8Builder,
    ask_price: Float32Builder,
    ask_condition: UInt8Builder,
    date: UInt32Builder,
    
    // Contract builders (repeated per tick)
    root: StringBuilder,
    expiration: UInt32Builder,
    strike: UInt32Builder,
    right: StringBuilder,
}

impl TickBatchBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            ms_of_day: UInt32Builder::with_capacity(capacity),
            bid_size: UInt16Builder::with_capacity(capacity),
            bid_exchange: UInt8Builder::with_capacity(capacity),
            bid_price: Float32Builder::with_capacity(capacity),
            bid_condition: UInt8Builder::with_capacity(capacity),
            ask_size: UInt16Builder::with_capacity(capacity),
            ask_exchange: UInt8Builder::with_capacity(capacity),
            ask_price: Float32Builder::with_capacity(capacity),
            ask_condition: UInt8Builder::with_capacity(capacity),
            date: UInt32Builder::with_capacity(capacity),
            root: StringBuilder::with_capacity(capacity, capacity * 8),
            expiration: UInt32Builder::with_capacity(capacity),
            strike: UInt32Builder::with_capacity(capacity),
            right: StringBuilder::with_capacity(capacity, capacity * 2),
        }
    }
    
    pub fn append_tick<T: TickData>(&mut self, tick: &T, contract: &JsonContract) {
        self.ms_of_day.append_value(tick.ms_of_day());
        self.bid_size.append_value(tick.bid_size() as u16);
        self.bid_exchange.append_value(tick.bid_exchange() as u8);
        self.bid_price.append_value(tick.bid_price() as f32);
        self.bid_condition.append_value(tick.bid_condition() as u8);
        self.ask_size.append_value(tick.ask_size() as u16);
        self.ask_exchange.append_value(tick.ask_exchange() as u8);
        self.ask_price.append_value(tick.ask_price() as f32);
        self.ask_condition.append_value(tick.ask_condition() as u8);
        self.date.append_value(tick.date());
        
        // Repeat contract info for each tick
        self.root.append_value(&contract.root);
        self.expiration.append_value(contract.expiration);
        self.strike.append_value(contract.strike);
        self.right.append_value(&contract.right);
    }
    
    pub fn finish(mut self) -> RecordBatch {
        let schema = Arc::new(create_tick_schema());
        
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.ms_of_day.finish()),
            Arc::new(self.bid_size.finish()),
            Arc::new(self.bid_exchange.finish()),
            Arc::new(self.bid_price.finish()),
            Arc::new(self.bid_condition.finish()),
            Arc::new(self.ask_size.finish()),
            Arc::new(self.ask_exchange.finish()),
            Arc::new(self.ask_price.finish()),
            Arc::new(self.ask_condition.finish()),
            Arc::new(self.date.finish()),
            Arc::new(self.root.finish()),
            Arc::new(self.expiration.finish()),
            Arc::new(self.strike.finish()),
            Arc::new(self.right.finish()),
        ];
        
        RecordBatch::try_new(schema, columns).expect("Failed to create RecordBatch")
    }
    
}
