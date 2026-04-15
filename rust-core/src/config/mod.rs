use rust_decimal::Decimal;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub venues: Vec<VenueConfig>,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    pub logging: LoggingConfig,
    #[serde(default)]
    pub engine: EngineConfig,
}

#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    #[serde(default = "default_reconcile_interval")]
    pub reconcile_interval_secs: u64,
    #[serde(default = "default_order_ttl")]
    pub order_ttl_secs: u64,
}

fn default_reconcile_interval() -> u64 {
    30
}

fn default_order_ttl() -> u64 {
    30
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            reconcile_interval_secs: default_reconcile_interval(),
            order_ttl_secs: default_order_ttl(),
        }
    }
}

#[derive(Deserialize)]
pub struct VenueConfig {
    pub name: String,
    pub market: String,
    pub api_key: String,
    pub api_secret: String,
    #[serde(default)]
    pub passphrase: Option<String>,
    pub paper_trading: bool,
    #[serde(default)]
    pub testnet: bool,
    #[serde(default)]
    pub fee_maker_override: Option<Decimal>,
    #[serde(default)]
    pub fee_taker_override: Option<Decimal>,
    #[serde(default)]
    pub instruments: Option<Vec<InstrumentConfig>>,
}

impl std::fmt::Debug for VenueConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VenueConfig")
            .field("name", &self.name)
            .field("market", &self.market)
            .field("api_key", &"[REDACTED]")
            .field("api_secret", &"[REDACTED]")
            .field(
                "passphrase",
                &self.passphrase.as_ref().map(|_| "[REDACTED]"),
            )
            .field("paper_trading", &self.paper_trading)
            .field("testnet", &self.testnet)
            .field("fee_maker_override", &self.fee_maker_override)
            .field("fee_taker_override", &self.fee_taker_override)
            .field("instruments", &self.instruments)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
pub struct StrategyConfig {
    pub name: String,
    pub instrument_a: InstrumentConfig,
    pub instrument_b: InstrumentConfig,
    pub min_net_profit_bps: Decimal,
    pub max_quantity: Decimal,
    pub max_quote_age_ms: i64,
    #[serde(default)]
    pub tick_size_a: Option<Decimal>,
    #[serde(default)]
    pub tick_size_b: Option<Decimal>,
    #[serde(default)]
    pub lot_size_a: Option<Decimal>,
    #[serde(default)]
    pub lot_size_b: Option<Decimal>,
    #[serde(default = "default_max_book_depth")]
    pub max_book_depth: usize,
    /// EwmaSpreadStrategy: EWMA smoothing factor α (0 < α < 1; default 0.05)
    #[serde(default)]
    pub ewma_alpha: Option<Decimal>,
    /// EwmaSpreadStrategy: sigma threshold for entry (default 2.0)
    #[serde(default)]
    pub ewma_entry_sigma: Option<Decimal>,
    /// EwmaSpreadStrategy: minimum samples before trading (default 60)
    #[serde(default)]
    pub ewma_min_samples: Option<u32>,
    /// FundingRateStrategy: annualized bps threshold to trigger (default = min_net_profit_bps)
    #[serde(default)]
    pub funding_min_bps: Option<Decimal>,
    /// TwEtfFuturesStrategy: hedge ratio (futures : ETF; default 1.0)
    #[serde(default)]
    pub tw_hedge_ratio: Option<Decimal>,
    /// TwEtfFuturesStrategy: annualized cost of carry in bps (default 0)
    #[serde(default)]
    pub tw_cost_of_carry_bps: Option<Decimal>,
    /// TwEtfFuturesStrategy: days to futures expiry (default 30)
    #[serde(default)]
    pub tw_days_to_expiry: Option<i64>,
    /// TriangularArbStrategy: list of A→B→C→A cycles
    #[serde(default)]
    pub triangle_cycles: Vec<TriangleCycleConfig>,
}

#[derive(Debug, Deserialize)]
pub struct TriangleCycleConfig {
    pub leg_a: TriangleLegConfig,
    pub leg_b: TriangleLegConfig,
    pub leg_c: TriangleLegConfig,
    #[serde(default = "default_triangle_max_notional")]
    pub max_notional_usdt: Decimal,
    #[serde(default)]
    pub min_net_profit_bps: Option<Decimal>,
    #[serde(default = "default_triangle_tick")]
    pub tick_size: Decimal,
    #[serde(default = "default_triangle_lot")]
    pub lot_size: Decimal,
}

#[derive(Debug, Deserialize)]
pub struct TriangleLegConfig {
    pub base: String,
    pub quote: String,
    pub side: String,
}

fn default_triangle_max_notional() -> Decimal {
    Decimal::new(1000, 0)
}

fn default_triangle_tick() -> Decimal {
    Decimal::new(1, 2)
}

fn default_triangle_lot() -> Decimal {
    Decimal::new(1, 5)
}

fn default_max_book_depth() -> usize {
    10
}

#[derive(Debug, Deserialize)]
pub struct InstrumentConfig {
    pub base: String,
    pub quote: String,
    pub instrument_type: String,
    pub settle_currency: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RiskConfig {
    pub max_position_size: Decimal,
    pub max_daily_loss: Decimal,
    pub max_notional_exposure: Decimal,
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,
}

#[derive(Debug, Deserialize)]
pub struct CircuitBreakerConfig {
    #[serde(default = "default_max_drawdown")]
    pub max_drawdown: Decimal,
    #[serde(default = "default_max_orders_per_minute")]
    pub max_orders_per_minute: u32,
    #[serde(default = "default_max_consecutive_failures")]
    pub max_consecutive_failures: u32,
}

fn default_max_drawdown() -> Decimal {
    Decimal::new(500, 0)
}

fn default_max_orders_per_minute() -> u32 {
    100
}

fn default_max_consecutive_failures() -> u32 {
    5
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            max_drawdown: default_max_drawdown(),
            max_orders_per_minute: default_max_orders_per_minute(),
            max_consecutive_failures: default_max_consecutive_failures(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    #[serde(default)]
    pub json_output: bool,
    pub log_file: Option<String>,
}

fn resolve_env_var(value: &str) -> String {
    if value.starts_with("${") && value.ends_with('}') {
        let var_name = &value[2..value.len() - 1];
        std::env::var(var_name).unwrap_or_default()
    } else if value.is_empty() {
        String::new()
    } else {
        value.to_string()
    }
}

pub fn load(path: &str) -> anyhow::Result<AppConfig> {
    let contents = fs::read_to_string(path)?;
    let mut config: AppConfig = serde_yaml::from_str(&contents)?;
    for venue in &mut config.venues {
        venue.api_key = resolve_env_var(&venue.api_key);
        venue.api_secret = resolve_env_var(&venue.api_secret);
        if let Some(ref pp) = venue.passphrase {
            venue.passphrase = Some(resolve_env_var(pp));
        }
    }
    validate(&config)?;
    Ok(config)
}

const KNOWN_STRATEGIES: &[&str] = &[
    "cross_exchange",
    "ewma_spread",
    "funding_rate",
    "triangular_arb",
    "tw_etf_futures",
];

const KNOWN_VENUES: &[&str] = &["binance", "bybit", "okx", "fubon", "shioaji"];

/// Structural validation beyond what serde can express. Runs after YAML parse.
/// Fails fast with a clear message instead of letting a misconfig reach runtime.
pub fn validate(config: &AppConfig) -> anyhow::Result<()> {
    if config.venues.is_empty() {
        anyhow::bail!("config.venues must not be empty");
    }
    for v in &config.venues {
        let name = v.name.to_lowercase();
        if !KNOWN_VENUES.contains(&name.as_str()) {
            anyhow::bail!(
                "unknown venue.name: '{}' (known: {})",
                v.name,
                KNOWN_VENUES.join(", ")
            );
        }
    }

    let strat = config.strategy.name.to_lowercase();
    if !KNOWN_STRATEGIES.contains(&strat.as_str()) {
        anyhow::bail!(
            "unknown strategy.name: '{}' (known: {})",
            config.strategy.name,
            KNOWN_STRATEGIES.join(", ")
        );
    }

    if config.strategy.max_quantity <= Decimal::ZERO {
        anyhow::bail!(
            "strategy.max_quantity must be > 0 (got {})",
            config.strategy.max_quantity
        );
    }
    if config.strategy.min_net_profit_bps < Decimal::ZERO {
        anyhow::bail!(
            "strategy.min_net_profit_bps must be >= 0 (got {})",
            config.strategy.min_net_profit_bps
        );
    }
    if config.strategy.max_quote_age_ms <= 0 {
        anyhow::bail!(
            "strategy.max_quote_age_ms must be > 0 (got {})",
            config.strategy.max_quote_age_ms
        );
    }

    if strat == "triangular_arb" {
        if config.strategy.triangle_cycles.is_empty() {
            anyhow::bail!("strategy.name = triangular_arb requires at least one triangle_cycle");
        }
        for (i, cycle) in config.strategy.triangle_cycles.iter().enumerate() {
            for (leg_name, leg) in [
                ("leg_a", &cycle.leg_a),
                ("leg_b", &cycle.leg_b),
                ("leg_c", &cycle.leg_c),
            ] {
                let side = leg.side.to_lowercase();
                if side != "buy" && side != "sell" {
                    anyhow::bail!(
                        "triangle_cycles[{}].{}.side must be 'buy' or 'sell' (got '{}')",
                        i,
                        leg_name,
                        leg.side
                    );
                }
            }
        }
    }

    if strat == "ewma_spread"
        && let Some(alpha) = config.strategy.ewma_alpha
        && (alpha <= Decimal::ZERO || alpha >= Decimal::ONE)
    {
        anyhow::bail!(
            "strategy.ewma_alpha must be in (0, 1) exclusive (got {})",
            alpha
        );
    }

    if config.risk.max_position_size <= Decimal::ZERO {
        anyhow::bail!(
            "risk.max_position_size must be > 0 (got {})",
            config.risk.max_position_size
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn sample_yaml() -> &'static str {
        r#"
venues:
  - name: binance
    market: spot
    api_key: ""
    api_secret: ""
    paper_trading: true

strategy:
  name: cross_exchange
  instrument_a:
    base: BTC
    quote: USDT
    instrument_type: spot
  instrument_b:
    base: BTC
    quote: USDT
    instrument_type: swap
    settle_currency: USDT
  min_net_profit_bps: "1"
  max_quantity: "0.01"
  max_quote_age_ms: 5000

risk:
  max_position_size: "1"
  max_daily_loss: "1000"
  max_notional_exposure: "100000"

logging:
  level: info
"#
    }

    #[test]
    fn parses_sample_config() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(sample_yaml().as_bytes()).unwrap();
        let cfg = load(f.path().to_str().unwrap()).unwrap();
        assert_eq!(cfg.venues.len(), 1);
        assert_eq!(cfg.venues[0].name, "binance");
        assert_eq!(cfg.strategy.name, "cross_exchange");
        assert_eq!(cfg.strategy.instrument_a.base, "BTC");
        assert_eq!(cfg.strategy.max_quote_age_ms, 5000);
        assert_eq!(cfg.risk.max_position_size, Decimal::new(1, 0));
    }

    #[test]
    fn resolves_env_var_pattern() {
        unsafe {
            std::env::set_var("TEST_ARBX_KEY", "secret123");
        }
        let result = resolve_env_var("${TEST_ARBX_KEY}");
        assert_eq!(result, "secret123");
        unsafe {
            std::env::remove_var("TEST_ARBX_KEY");
        }
    }

    #[test]
    fn empty_string_stays_empty() {
        assert_eq!(resolve_env_var(""), "");
    }

    #[test]
    fn plain_string_passes_through() {
        assert_eq!(resolve_env_var("my_key"), "my_key");
    }

    fn config_with(strategy_name: &str, venue_name: &str) -> String {
        sample_yaml()
            .replace("name: cross_exchange", &format!("name: {}", strategy_name))
            .replace("- name: binance", &format!("- name: {}", venue_name))
    }

    fn try_load(yaml: &str) -> anyhow::Result<AppConfig> {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(yaml.as_bytes()).unwrap();
        load(f.path().to_str().unwrap())
    }

    #[test]
    fn rejects_unknown_strategy_name() {
        let yaml = config_with("does_not_exist", "binance");
        let err = try_load(&yaml).unwrap_err().to_string();
        assert!(err.contains("unknown strategy.name"), "got: {err}");
        assert!(err.contains("does_not_exist"), "got: {err}");
    }

    #[test]
    fn rejects_unknown_venue_name() {
        let yaml = config_with("cross_exchange", "totally_fake");
        let err = try_load(&yaml).unwrap_err().to_string();
        assert!(err.contains("unknown venue.name"), "got: {err}");
    }

    #[test]
    fn rejects_triangular_arb_without_cycles() {
        let yaml = config_with("triangular_arb", "binance");
        let err = try_load(&yaml).unwrap_err().to_string();
        assert!(err.contains("triangular_arb requires"), "got: {err}");
    }

    #[test]
    fn rejects_ewma_alpha_out_of_range() {
        let yaml = sample_yaml().replace(
            "name: cross_exchange",
            "name: ewma_spread\n  ewma_alpha: \"1.5\"",
        );
        let err = try_load(&yaml).unwrap_err().to_string();
        assert!(err.contains("ewma_alpha"), "got: {err}");
        assert!(err.contains("(0, 1)"), "got: {err}");
    }

    #[test]
    fn rejects_non_positive_max_quantity() {
        let yaml = sample_yaml().replace("max_quantity: \"0.01\"", "max_quantity: \"0\"");
        let err = try_load(&yaml).unwrap_err().to_string();
        assert!(err.contains("max_quantity must be > 0"), "got: {err}");
    }

    #[test]
    fn rejects_negative_min_net_profit_bps() {
        let yaml = sample_yaml().replace("min_net_profit_bps: \"1\"", "min_net_profit_bps: \"-5\"");
        let err = try_load(&yaml).unwrap_err().to_string();
        assert!(
            err.contains("min_net_profit_bps must be >= 0"),
            "got: {err}"
        );
    }
}
