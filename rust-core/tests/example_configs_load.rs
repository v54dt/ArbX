//! Loads each shipped example config under `config/` and asserts the schema
//! still deserializes. Catches drift between StrategyConfig fields and the
//! YAML examples we ship.

use std::path::PathBuf;

use arbx_core::config;

fn project_config_dir() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.push("config");
    p
}

fn load(name: &str) -> anyhow::Result<config::AppConfig> {
    let mut p = project_config_dir();
    p.push(name);
    config::load(p.to_str().unwrap())
}

#[test]
fn default_config_loads() {
    let cfg = load("default.yaml").expect("default.yaml");
    assert_eq!(cfg.strategy.name, "cross_exchange");
}

#[test]
fn ewma_spread_config_loads() {
    let cfg = load("ewma_spread.yaml").expect("ewma_spread.yaml");
    assert_eq!(cfg.strategy.name, "ewma_spread");
    assert!(cfg.strategy.ewma_alpha.is_some());
    assert!(cfg.strategy.ewma_entry_sigma.is_some());
}

#[test]
fn funding_rate_config_loads() {
    let cfg = load("funding_rate.yaml").expect("funding_rate.yaml");
    assert_eq!(cfg.strategy.name, "funding_rate");
}

#[test]
fn triangular_config_loads_with_cycles() {
    let cfg = load("triangular.yaml").expect("triangular.yaml");
    assert_eq!(cfg.strategy.name, "triangular_arb");
    assert!(
        !cfg.strategy.triangle_cycles.is_empty(),
        "triangular.yaml must define at least one cycle"
    );
    let c = &cfg.strategy.triangle_cycles[0];
    assert!(matches!(
        c.leg_a.side.to_lowercase().as_str(),
        "buy" | "sell"
    ));
}

#[test]
fn okx_config_loads() {
    load("okx.yaml").expect("okx.yaml");
}

#[test]
fn bybit_config_loads() {
    load("bybit.yaml").expect("bybit.yaml");
}

#[test]
fn testnet_config_loads() {
    load("testnet.yaml").expect("testnet.yaml");
}
