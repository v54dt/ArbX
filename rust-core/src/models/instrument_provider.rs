use std::collections::HashMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::models::enums::Venue;
use crate::models::instrument::{AssetClass, Instrument, InstrumentType};

/// Per-instrument contract specification loaded from config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractSpec {
    pub venue: Venue,
    pub symbol: String,
    pub instrument: Instrument,
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    #[serde(default = "default_multiplier")]
    pub multiplier: Decimal,
    /// For futures: expiry date string (e.g. "2026-06-18").
    #[serde(default)]
    pub expiry: Option<String>,
    /// Daily price limit as fraction (e.g. 0.10 for +/-10%).
    #[serde(default)]
    pub price_limit_pct: Option<Decimal>,
}

fn default_multiplier() -> Decimal {
    Decimal::ONE
}

/// Catalog of all known instruments. Keyed by book_key format
/// ("{venue}:{base}-{quote}:{type}" lowercased).
#[derive(Debug, Clone, Default)]
pub struct InstrumentProvider {
    specs: HashMap<String, ContractSpec>,
}

impl InstrumentProvider {
    pub fn new() -> Self {
        Self {
            specs: HashMap::new(),
        }
    }

    pub fn register(&mut self, key: String, spec: ContractSpec) {
        self.specs.insert(key, spec);
    }

    pub fn get(&self, key: &str) -> Option<&ContractSpec> {
        self.specs.get(key)
    }

    pub fn tick_size(&self, key: &str) -> Option<Decimal> {
        self.specs.get(key).map(|s| s.tick_size)
    }

    pub fn lot_size(&self, key: &str) -> Option<Decimal> {
        self.specs.get(key).map(|s| s.lot_size)
    }

    pub fn multiplier(&self, key: &str) -> Option<Decimal> {
        self.specs.get(key).map(|s| s.multiplier)
    }

    pub fn len(&self) -> usize {
        self.specs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    /// Round a price down to the nearest tick.
    pub fn tick_round(&self, key: &str, price: Decimal) -> Decimal {
        match self.tick_size(key) {
            Some(tick) if !tick.is_zero() => (price / tick).floor() * tick,
            _ => price,
        }
    }

    /// Round a quantity down to the nearest lot.
    pub fn lot_round(&self, key: &str, qty: Decimal) -> Decimal {
        match self.lot_size(key) {
            Some(lot) if !lot.is_zero() => (qty / lot).floor() * lot,
            _ => qty,
        }
    }
}

// ---------------------------------------------------------------------------
// YAML loading helpers
// ---------------------------------------------------------------------------

/// Raw row from `contracts.yaml`. String fields converted to enums via
/// `into_contract_spec`.
#[derive(Debug, Deserialize)]
struct RawContractRow {
    venue: String,
    symbol: String,
    base: String,
    quote: String,
    instrument_type: String,
    tick_size: Decimal,
    lot_size: Decimal,
    #[serde(default = "default_multiplier")]
    multiplier: Decimal,
    #[serde(default)]
    expiry: Option<String>,
    #[serde(default)]
    price_limit_pct: Option<Decimal>,
}

#[derive(Debug, Deserialize)]
struct RawContractsFile {
    contracts: Vec<RawContractRow>,
}

fn parse_venue(name: &str) -> Result<Venue, String> {
    match name.to_lowercase().as_str() {
        "binance" => Ok(Venue::Binance),
        "bybit" => Ok(Venue::Bybit),
        "okx" => Ok(Venue::Okx),
        "fubon" => Ok(Venue::Fubon),
        "shioaji" => Ok(Venue::Shioaji),
        other => Err(format!("unknown venue: {other}")),
    }
}

fn parse_instrument_type(s: &str) -> Result<InstrumentType, String> {
    match s.to_lowercase().as_str() {
        "spot" => Ok(InstrumentType::Spot),
        "futures" => Ok(InstrumentType::Futures),
        "swap" => Ok(InstrumentType::Swap),
        "option" => Ok(InstrumentType::Option),
        other => Err(format!("unknown instrument_type: {other}")),
    }
}

fn asset_class_for_venue(venue: Venue) -> AssetClass {
    match venue {
        Venue::Binance | Venue::Bybit | Venue::Okx => AssetClass::Crypto,
        Venue::Shioaji | Venue::Fubon => AssetClass::Equity,
    }
}

/// Load an `InstrumentProvider` from a YAML string (the contents of
/// `config/contracts.yaml`).
pub fn load_contracts_yaml(yaml: &str) -> Result<InstrumentProvider, String> {
    let raw: RawContractsFile =
        serde_yaml::from_str(yaml).map_err(|e| format!("YAML parse error: {e}"))?;
    let mut provider = InstrumentProvider::new();
    for row in raw.contracts {
        let venue = parse_venue(&row.venue)?;
        let itype = parse_instrument_type(&row.instrument_type)?;
        let instrument = Instrument {
            asset_class: asset_class_for_venue(venue),
            instrument_type: itype,
            base: row.base.clone(),
            quote: row.quote.clone(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        };
        let key = crate::models::market::book_key(venue, &instrument);
        let spec = ContractSpec {
            venue,
            symbol: row.symbol,
            instrument,
            tick_size: row.tick_size,
            lot_size: row.lot_size,
            multiplier: row.multiplier,
            expiry: row.expiry,
            price_limit_pct: row.price_limit_pct,
        };
        provider.register(key.to_string(), spec);
    }
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::instrument::{AssetClass, InstrumentType};
    use crate::models::market::book_key;
    use rust_decimal_macros::dec;

    fn make_instrument(base: &str, quote: &str, itype: InstrumentType) -> Instrument {
        Instrument {
            asset_class: AssetClass::Crypto,
            instrument_type: itype,
            base: base.into(),
            quote: quote.into(),
            settle_currency: None,
            expiry: None,
            last_trade_time: None,
            settlement_time: None,
        }
    }

    fn make_spec(venue: Venue, symbol: &str, inst: Instrument) -> ContractSpec {
        ContractSpec {
            venue,
            symbol: symbol.into(),
            instrument: inst,
            tick_size: dec!(0.01),
            lot_size: dec!(0.001),
            multiplier: Decimal::ONE,
            expiry: None,
            price_limit_pct: None,
        }
    }

    #[test]
    fn register_and_get() {
        let mut provider = InstrumentProvider::new();
        let inst = make_instrument("BTC", "USDT", InstrumentType::Swap);
        let key = book_key(Venue::Binance, &inst).to_string();
        let spec = make_spec(Venue::Binance, "BTCUSDT", inst);
        provider.register(key.clone(), spec);

        let got = provider.get(&key).unwrap();
        assert_eq!(got.symbol, "BTCUSDT");
        assert_eq!(got.venue, Venue::Binance);
        assert_eq!(got.tick_size, dec!(0.01));
        assert_eq!(got.lot_size, dec!(0.001));
    }

    #[test]
    fn get_returns_none_for_unknown_key() {
        let provider = InstrumentProvider::new();
        assert!(provider.get("nonexistent").is_none());
    }

    #[test]
    fn default_multiplier_is_one() {
        let mut provider = InstrumentProvider::new();
        let inst = make_instrument("ETH", "USDT", InstrumentType::Spot);
        let key = book_key(Venue::Binance, &inst).to_string();
        provider.register(key.clone(), make_spec(Venue::Binance, "ETHUSDT", inst));

        assert_eq!(provider.multiplier(&key), Some(Decimal::ONE));
    }

    #[test]
    fn tick_round_basic() {
        let mut provider = InstrumentProvider::new();
        let inst = make_instrument("BTC", "USDT", InstrumentType::Swap);
        let key = book_key(Venue::Binance, &inst).to_string();
        let mut spec = make_spec(Venue::Binance, "BTCUSDT", inst);
        spec.tick_size = dec!(0.10);
        provider.register(key.clone(), spec);

        assert_eq!(provider.tick_round(&key, dec!(100.37)), dec!(100.30));
        assert_eq!(provider.tick_round(&key, dec!(100.30)), dec!(100.30));
        assert_eq!(provider.tick_round(&key, dec!(100.00)), dec!(100.00));
    }

    #[test]
    fn tick_round_small_tick() {
        let mut provider = InstrumentProvider::new();
        let inst = make_instrument("ETH", "USDT", InstrumentType::Swap);
        let key = book_key(Venue::Binance, &inst).to_string();
        let mut spec = make_spec(Venue::Binance, "ETHUSDT", inst);
        spec.tick_size = dec!(0.01);
        provider.register(key.clone(), spec);

        assert_eq!(provider.tick_round(&key, dec!(3456.789)), dec!(3456.78));
    }

    #[test]
    fn tick_round_unknown_key_returns_price_unchanged() {
        let provider = InstrumentProvider::new();
        assert_eq!(provider.tick_round("missing", dec!(123.456)), dec!(123.456));
    }

    #[test]
    fn lot_round_basic() {
        let mut provider = InstrumentProvider::new();
        let inst = make_instrument("BTC", "USDT", InstrumentType::Swap);
        let key = book_key(Venue::Binance, &inst).to_string();
        let mut spec = make_spec(Venue::Binance, "BTCUSDT", inst);
        spec.lot_size = dec!(0.001);
        provider.register(key.clone(), spec);

        assert_eq!(provider.lot_round(&key, dec!(1.2345)), dec!(1.234));
        assert_eq!(provider.lot_round(&key, dec!(1.001)), dec!(1.001));
        assert_eq!(provider.lot_round(&key, dec!(0.0005)), dec!(0.000));
    }

    #[test]
    fn lot_round_whole_lots() {
        let mut provider = InstrumentProvider::new();
        let inst = make_instrument("2330", "TWD", InstrumentType::Spot);
        let key = book_key(Venue::Shioaji, &inst).to_string();
        let mut spec = make_spec(Venue::Shioaji, "2330", inst);
        spec.lot_size = dec!(1000);
        provider.register(key.clone(), spec);

        assert_eq!(provider.lot_round(&key, dec!(2500)), dec!(2000));
        assert_eq!(provider.lot_round(&key, dec!(999)), dec!(0));
    }

    #[test]
    fn len_and_is_empty() {
        let mut provider = InstrumentProvider::new();
        assert!(provider.is_empty());
        assert_eq!(provider.len(), 0);

        let inst = make_instrument("BTC", "USDT", InstrumentType::Spot);
        let key = book_key(Venue::Binance, &inst).to_string();
        provider.register(key, make_spec(Venue::Binance, "BTCUSDT", inst));
        assert!(!provider.is_empty());
        assert_eq!(provider.len(), 1);
    }

    #[test]
    fn load_contracts_yaml_roundtrip() {
        let yaml = r#"
contracts:
  - venue: binance
    symbol: BTCUSDT
    base: BTC
    quote: USDT
    instrument_type: swap
    tick_size: "0.10"
    lot_size: "0.001"
    multiplier: "1"
  - venue: shioaji
    symbol: TXF
    base: TAIEX
    quote: TWD
    instrument_type: futures
    tick_size: "1"
    lot_size: "1"
    multiplier: "200"
    expiry: "2026-06-17"
    price_limit_pct: "0.10"
"#;
        let provider = load_contracts_yaml(yaml).unwrap();
        assert_eq!(provider.len(), 2);

        let btc_key = "binance:btc-usdt:swap";
        let btc = provider.get(btc_key).unwrap();
        assert_eq!(btc.tick_size, dec!(0.10));
        assert_eq!(btc.lot_size, dec!(0.001));
        assert_eq!(btc.multiplier, Decimal::ONE);
        assert!(btc.expiry.is_none());

        let txf_key = "shioaji:taiex-twd:futures";
        let txf = provider.get(txf_key).unwrap();
        assert_eq!(txf.tick_size, dec!(1));
        assert_eq!(txf.multiplier, dec!(200));
        assert_eq!(txf.expiry.as_deref(), Some("2026-06-17"));
        assert_eq!(txf.price_limit_pct, Some(dec!(0.10)));
    }

    #[test]
    fn load_contracts_yaml_bad_venue() {
        let yaml = r#"
contracts:
  - venue: kraken
    symbol: X
    base: X
    quote: Y
    instrument_type: spot
    tick_size: "1"
    lot_size: "1"
"#;
        let err = load_contracts_yaml(yaml).unwrap_err();
        assert!(err.contains("unknown venue"), "got: {err}");
    }
}
