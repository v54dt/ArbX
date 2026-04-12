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
}

fn default_reconcile_interval() -> u64 {
    30
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            reconcile_interval_secs: default_reconcile_interval(),
        }
    }
}

#[derive(Deserialize)]
pub struct VenueConfig {
    pub name: String,
    pub market: String,
    pub api_key: String,
    pub api_secret: String,
    pub paper_trading: bool,
}

impl std::fmt::Debug for VenueConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VenueConfig")
            .field("name", &self.name)
            .field("market", &self.market)
            .field("api_key", &"[REDACTED]")
            .field("api_secret", &"[REDACTED]")
            .field("paper_trading", &self.paper_trading)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
pub struct StrategyConfig {
    #[allow(dead_code)] // used for config identification
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
    }
    Ok(config)
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
}
