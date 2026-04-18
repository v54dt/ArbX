use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use super::enums::{Side, Venue};
use super::instrument::Instrument;

/// Records one leg of an executed (or attempted) arbitrage trade.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeLeg {
    pub venue: Venue,
    pub instrument: Instrument,
    pub side: Side,
    pub intended_price: Decimal,
    pub intended_quantity: Decimal,
    pub order_id: Option<String>,
    pub submitted_at: DateTime<Utc>,
}

/// Outcome of a single arbitrage execution attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeOutcome {
    AllSubmitted,
    PartialFailure,
    RiskRejected,
}

/// A paired record of one arbitrage opportunity that was acted upon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeLog {
    pub id: String,
    pub strategy_id: String,
    pub outcome: TradeOutcome,
    pub legs: SmallVec<[TradeLeg; 4]>,
    pub expected_gross_profit: Decimal,
    pub expected_fees: Decimal,
    pub expected_net_profit: Decimal,
    pub expected_net_profit_bps: Decimal,
    pub notional: Decimal,
    pub created_at: DateTime<Utc>,
}

/// Append-only JSONL writer for TradeLog records. One JSON object per line,
/// flushed per append so a crash mid-session still leaves the prior logs on disk.
pub struct TradeLogWriter {
    writer: std::io::BufWriter<std::fs::File>,
    base_path: String,
    current_date: String,
}

impl TradeLogWriter {
    pub fn create(path: &str) -> anyhow::Result<Self> {
        let date = chrono::Utc::now()
            .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
            .format("%Y-%m-%d")
            .to_string();
        let actual_path = Self::dated_path(path, &date);
        if let Some(parent) = std::path::Path::new(&actual_path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&actual_path)?;
        Ok(Self {
            writer: std::io::BufWriter::new(file),
            base_path: path.to_string(),
            current_date: date,
        })
    }

    fn dated_path(base: &str, date: &str) -> String {
        if let Some(dot) = base.rfind('.') {
            format!("{}-{}{}", &base[..dot], date, &base[dot..])
        } else {
            format!("{}-{}", base, date)
        }
    }

    fn maybe_rotate(&mut self) -> anyhow::Result<()> {
        let today = chrono::Utc::now()
            .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
            .format("%Y-%m-%d")
            .to_string();
        if today != self.current_date {
            use std::io::Write as _;
            self.writer.flush()?;
            let new_path = Self::dated_path(&self.base_path, &today);
            if let Some(parent) = std::path::Path::new(&new_path).parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)?;
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&new_path)?;
            self.writer = std::io::BufWriter::new(file);
            self.current_date = today;
            tracing::info!(path = new_path.as_str(), "trade_log rotated to new day");
        }
        Ok(())
    }

    pub fn append(&mut self, log: &TradeLog) -> anyhow::Result<()> {
        use std::io::Write as _;
        self.maybe_rotate()?;
        let line = serde_json::to_string(log)?;
        self.writer.write_all(line.as_bytes())?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::instrument::{AssetClass, Instrument, InstrumentType};
    use rust_decimal_macros::dec;
    use smallvec::smallvec;

    fn sample_trade_log() -> TradeLog {
        TradeLog {
            id: "t1".into(),
            strategy_id: "s1".into(),
            outcome: TradeOutcome::AllSubmitted,
            legs: smallvec![TradeLeg {
                venue: Venue::Binance,
                instrument: Instrument {
                    asset_class: AssetClass::Crypto,
                    instrument_type: InstrumentType::Spot,
                    base: "BTC".into(),
                    quote: "USDT".into(),
                    settle_currency: None,
                    expiry: None,
                    last_trade_time: None,
                    settlement_time: None,
                },
                side: Side::Buy,
                intended_price: dec!(50000),
                intended_quantity: dec!(1),
                order_id: Some("o1".into()),
                submitted_at: Utc::now(),
            }],
            expected_gross_profit: dec!(10),
            expected_fees: dec!(2),
            expected_net_profit: dec!(8),
            expected_net_profit_bps: dec!(5),
            notional: dec!(50000),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn trade_log_serialization_roundtrip() {
        let log = sample_trade_log();
        let json = serde_json::to_string(&log).unwrap();
        let de: TradeLog = serde_json::from_str(&json).unwrap();
        assert_eq!(de.id, log.id);
        assert_eq!(de.strategy_id, log.strategy_id);
        assert_eq!(de.outcome, log.outcome);
        assert_eq!(de.legs.len(), 1);
        assert_eq!(de.expected_net_profit, dec!(8));
        assert_eq!(de.notional, dec!(50000));
    }

    #[test]
    fn trade_outcome_variants() {
        for variant in [
            TradeOutcome::AllSubmitted,
            TradeOutcome::PartialFailure,
            TradeOutcome::RiskRejected,
        ] {
            let json = serde_json::to_value(variant).unwrap();
            let de: TradeOutcome = serde_json::from_value(json).unwrap();
            assert_eq!(de, variant);
        }
    }

    #[test]
    fn writer_appends_one_jsonl_line_per_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trades.jsonl");
        let mut w = TradeLogWriter::create(path.to_str().unwrap()).unwrap();
        w.append(&sample_trade_log()).unwrap();
        w.append(&sample_trade_log()).unwrap();
        drop(w);

        let body = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2);
        let parsed: TradeLog = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed.id, "t1");
    }

    #[test]
    fn writer_creates_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/subdir/trades.jsonl");
        let mut w = TradeLogWriter::create(path.to_str().unwrap()).unwrap();
        w.append(&sample_trade_log()).unwrap();
        drop(w);

        assert!(path.exists());
    }

    #[test]
    fn writer_append_preserves_prior_contents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trades.jsonl");
        {
            let mut w = TradeLogWriter::create(path.to_str().unwrap()).unwrap();
            w.append(&sample_trade_log()).unwrap();
        }
        {
            // Re-open: should append, not truncate.
            let mut w = TradeLogWriter::create(path.to_str().unwrap()).unwrap();
            w.append(&sample_trade_log()).unwrap();
        }
        let lines = std::fs::read_to_string(&path).unwrap().lines().count();
        assert_eq!(lines, 2);
    }
}
