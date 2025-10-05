use implied_vol::{DefaultSpecialFn, ImpliedBlackVolatility};
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rayon::prelude::*;
use statrs::distribution::{ContinuousCDF, Normal};

/// Input for implied volatility calculations (uses forward price)
#[derive(Debug, Clone)]
pub struct VolatilityInput {
    pub price: f64,
    pub forward: f64,
    pub strike: f64,
    pub dte: f64,
    pub is_call: bool,
}

/// Input for Greeks calculations (uses spot price and risk-free rate)
#[derive(Debug, Clone)]
pub struct GreeksInput {
    pub price: f64,
    pub spot: f64,
    pub strike: f64,
    pub dte: f64,
    pub volatility: f64,
    pub risk_free_rate: f64,
    pub is_call: bool,
}

/// Result with index tracking for parallel processing
#[derive(Debug)]
pub struct VolatilityResult {
    pub iv: f64,
    pub index: usize,
}

/// Greeks calculation results
#[derive(Debug)]
pub struct GreeksResult {
    pub delta: f64,
    pub gamma: f64,
    pub theta: f64,
    pub vega: f64,
    pub rho: f64,
    pub index: usize,
}

/// Extract VolatilityInput structs from DataFrame
fn extract_volatility_inputs(
    df: &DataFrame,
    price_col: &str,
    forward_col: &str,
) -> PolarsResult<Vec<VolatilityInput>> {
    let len = df.height();
    let mut inputs = Vec::with_capacity(len);

    let prices = df.column(price_col)?.f64()?;
    let forwards = df.column(forward_col)?.f64()?;
    let strikes = df.column("strike")?.f64()?;
    let dtes = df.column("dte")?.f64()?;
    let is_calls = df.column("is_call")?.bool()?;

    for i in 0..len {
        inputs.push(VolatilityInput {
            price: prices.get(i).unwrap(),
            forward: forwards.get(i).unwrap(),
            strike: strikes.get(i).unwrap(),
            dte: dtes.get(i).unwrap(),
            is_call: is_calls.get(i).unwrap(),
        });
    }

    Ok(inputs)
}

// /// Extract GreeksInput structs from DataFrame
fn extract_greeks_inputs(
    df: &DataFrame,
    price_col: &str,
    spot_col: &str,
    volatility_col: &str,
    risk_free_col: &str,
) -> PolarsResult<Vec<GreeksInput>> {
    let len = df.height();
    let mut inputs = Vec::with_capacity(len);

    let prices = df.column(price_col)?.f64()?;
    let spots = df.column(spot_col)?.f64()?;
    let strikes = df.column("strike")?.f64()?;
    let dtes = df.column("dte")?.f64()?;
    let volatilities = df.column(volatility_col)?.f64()?;
    let risk_free_rates = df.column(risk_free_col)?.f64()?;
    let is_calls = df.column("is_call")?.bool()?;

    for i in 0..len {
        inputs.push(GreeksInput {
            price: prices.get(i).unwrap(),
            spot: spots.get(i).unwrap(),
            strike: strikes.get(i).unwrap(),
            dte: dtes.get(i).unwrap(),
            volatility: volatilities.get(i).unwrap(),
            risk_free_rate: risk_free_rates.get(i).unwrap(),
            is_call: is_calls.get(i).unwrap(),
        });
    }

    Ok(inputs)
}

// /// Parallel batch calculation of implied volatility
fn calc_implied_volatility_batch(inputs: &[VolatilityInput]) -> PyResult<Vec<VolatilityResult>> {
    inputs
        .par_iter()
        .enumerate()
        .map(|(index, input)| {
            let builder = ImpliedBlackVolatility::builder()
                .option_price(input.price)
                .forward(input.forward)
                .strike(input.strike)
                .expiry(input.dte)
                .is_call(input.is_call);

            let iv_builder = builder.build().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to build IV Calculatation"
                ))
            })?;

            let iv = iv_builder.calculate::<DefaultSpecialFn>().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to calculate implied volatility"
                ))
            })?;

            Ok(VolatilityResult { iv, index })
        })
        .collect()
}

// /// Parallel batch calculation of Greeks
fn calc_greeks_batch(inputs: &[GreeksInput]) -> Vec<GreeksResult> {
    inputs
        .par_iter()
        .enumerate()
        .map(|(index, input)| {
            // Black-Scholes Greeks calculations
            let greeks = calculate_greeks(
                input.spot,
                input.strike,
                input.dte,
                input.risk_free_rate,
                input.volatility,
                input.is_call,
            );

            GreeksResult {
                delta: greeks.0,
                gamma: greeks.1,
                theta: greeks.2,
                vega: greeks.3,
                rho: greeks.4,
                index,
            }
        })
        .collect()
}

/// Calculate Black-Scholes Greeks
fn calculate_greeks(
    spot: f64,
    strike: f64,
    time_to_expiry: f64,
    risk_free_rate: f64,
    volatility: f64,
    is_call: bool,
) -> (f64, f64, f64, f64, f64) {
    use std::f64::consts::PI;

    let sqrt_t = time_to_expiry.sqrt();
    let d1 = ((spot / strike).ln()
        + (risk_free_rate + 0.5 * volatility * volatility) * time_to_expiry)
        / (volatility * sqrt_t);
    let d2 = d1 - volatility * sqrt_t;

    // Standard normal PDF
    let n_prime_d1 = (-0.5 * d1 * d1).exp() / (2.0 * PI).sqrt();

    let n_d1 = Normal::standard().cdf(d1);
    let n_d2 = Normal::standard().cdf(d2);

    // Delta
    let delta = if is_call { n_d1 } else { n_d1 - 1.0 };

    // Gamma (same for calls and puts)
    let gamma = n_prime_d1 / (spot * volatility * sqrt_t);

    // Theta
    let theta = if is_call {
        (-spot * n_prime_d1 * volatility / (2.0 * sqrt_t))
            - risk_free_rate * strike * (-risk_free_rate * time_to_expiry).exp() * n_d2
    } else {
        (-spot * n_prime_d1 * volatility / (2.0 * sqrt_t))
            + risk_free_rate * strike * (-risk_free_rate * time_to_expiry).exp() * (1.0 - n_d2)
    };

    // Vega (same for calls and puts)
    let vega = spot * sqrt_t * n_prime_d1;

    // Rho
    let rho = if is_call {
        strike * time_to_expiry * (-risk_free_rate * time_to_expiry).exp() * n_d2
    } else {
        -strike * time_to_expiry * (-risk_free_rate * time_to_expiry).exp() * (1.0 - n_d2)
    };

    (delta, gamma, theta / 365.0, vega / 100.0, rho / 100.0) // Normalize theta and vega
}

#[pyfunction]
fn add_implied_volatility(
    py_df: PyDataFrame,
    price_col: Option<String>,
    forward_col: Option<String>,
) -> PyResult<PyDataFrame> {
    let mut df: DataFrame = py_df.into();

    let price_str = price_col.unwrap_or_else(|| "price".to_string());
    let forward_str = forward_col.unwrap_or_else(|| "forward".to_string());

    // Extract inputs
    let vol_inputs = extract_volatility_inputs(&df, &price_str, &forward_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    // Calculate in parallel
    let mut results = calc_implied_volatility_batch(&vol_inputs)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    // Sort by index to preserve order
    results.sort_unstable_by_key(|r| r.index);

    // Extract values
    let implied_vols: Vec<f64> = results.into_iter().map(|r| r.iv).collect();

    // Add column
    df.with_column(Series::new("implied_volatility".into(), implied_vols))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    Ok(PyDataFrame(df))
}

/// Adds Greek columns to a DataFrame
#[pyfunction]
fn add_greeks(
    py_df: PyDataFrame,
    price_col: Option<String>,
    spot_col: Option<String>,
    volatility_col: Option<String>,
    risk_free_col: Option<String>,
) -> PyResult<PyDataFrame> {
    let df: DataFrame = py_df.into();

    let price_str = price_col.unwrap_or_else(|| "price".to_string());
    let spot_str = spot_col.unwrap_or_else(|| "spot".to_string());
    let vol_str = volatility_col.unwrap_or_else(|| "implied_volatility".to_string());
    let rf_str = risk_free_col.unwrap_or_else(|| "risk_free_rate".to_string());

    // Extract inputs
    let greeks_inputs = extract_greeks_inputs(&df, &price_str, &spot_str, &vol_str, &rf_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    // Calculate in parallel
    let mut results = calc_greeks_batch(&greeks_inputs);

    // Sort by index to preserve order
    results.sort_unstable_by_key(|r| r.index);

    // Extract values and create series
    let deltas: Vec<f64> = results.iter().map(|r| r.delta).collect();
    let gammas: Vec<f64> = results.iter().map(|r| r.gamma).collect();
    let thetas: Vec<f64> = results.iter().map(|r| r.theta).collect();
    let vegas: Vec<f64> = results.iter().map(|r| r.vega).collect();
    let rhos: Vec<f64> = results.iter().map(|r| r.rho).collect();

    // Create all Greek columns
    let greek_columns = vec![
        Column::new("delta".into(), deltas),
        Column::new("gamma".into(), gammas),
        Column::new("theta".into(), thetas),
        Column::new("vega".into(), vegas),
        Column::new("rho".into(), rhos),
    ];

    // Add all columns at once
    let df_with_greeks = df.hstack(&greek_columns)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    Ok(PyDataFrame(df_with_greeks))
}

/// A Python module implemented in Rust
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(add_implied_volatility, m)?)?;
    m.add_function(wrap_pyfunction!(add_greeks, m)?)?;
    Ok(())
}
