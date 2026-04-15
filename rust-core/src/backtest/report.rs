use std::fmt::Write as _;
use std::io::Write as _;

use super::engine::BacktestResult;
use crate::models::trade_log::{TradeLog, TradeOutcome};

pub fn print_report(result: &BacktestResult) {
    println!("=== Backtest Report ===");
    println!(
        "Trades: {} ({} profitable)",
        result.total_trades, result.profitable_trades
    );
    println!("Total PnL: {}", result.total_pnl);
    println!("Max Drawdown: {}", result.max_drawdown);
    println!("Sharpe Ratio: {:.2}", result.sharpe_ratio);
    println!("Duration: {}ms", result.duration_ms);
}

pub fn render_trade_csv(logs: &[TradeLog]) -> String {
    let mut buf = String::with_capacity(256 * (logs.len() + 1));
    buf.push_str(
        "id,strategy_id,outcome,gross_profit,fees,net_profit,net_profit_bps,notional,legs,created_at\n",
    );
    for log in logs {
        let outcome = match log.outcome {
            TradeOutcome::AllSubmitted => "AllSubmitted",
            TradeOutcome::PartialFailure => "PartialFailure",
            TradeOutcome::RiskRejected => "RiskRejected",
        };
        let _ = writeln!(
            buf,
            "{},{},{},{},{},{},{},{},{},{}",
            log.id,
            log.strategy_id,
            outcome,
            log.expected_gross_profit,
            log.expected_fees,
            log.expected_net_profit,
            log.expected_net_profit_bps,
            log.notional,
            log.legs.len(),
            log.created_at.to_rfc3339(),
        );
    }
    buf
}

pub fn write_trade_csv(path: &str, logs: &[TradeLog]) -> anyhow::Result<()> {
    let buf = render_trade_csv(logs);
    let mut file = std::fs::File::create(path)?;
    file.write_all(buf.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use smallvec::smallvec;

    fn fake_log(id: &str, outcome: TradeOutcome, net: rust_decimal::Decimal) -> TradeLog {
        TradeLog {
            id: id.into(),
            strategy_id: "test".into(),
            outcome,
            legs: smallvec![],
            expected_gross_profit: net + dec!(1),
            expected_fees: dec!(1),
            expected_net_profit: net,
            expected_net_profit_bps: dec!(0),
            notional: dec!(50000),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn render_csv_has_header_and_row_per_log() {
        let logs = vec![
            fake_log("a", TradeOutcome::AllSubmitted, dec!(10)),
            fake_log("b", TradeOutcome::RiskRejected, dec!(0)),
        ];
        let csv = render_trade_csv(&logs);
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines.len(), 3, "header + 2 rows");
        assert!(lines[0].starts_with("id,strategy_id,outcome,"));
        assert!(lines[1].starts_with("a,test,AllSubmitted,"));
        assert!(lines[2].starts_with("b,test,RiskRejected,"));
    }

    #[test]
    fn render_csv_empty_logs_keeps_header() {
        let csv = render_trade_csv(&[]);
        assert_eq!(csv.lines().count(), 1);
    }

    #[test]
    fn write_trade_csv_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.csv");
        let logs = vec![fake_log("x", TradeOutcome::PartialFailure, dec!(-5))];
        write_trade_csv(path.to_str().unwrap(), &logs).unwrap();
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("PartialFailure"));
        assert!(body.contains("x,test,"));
    }
}
