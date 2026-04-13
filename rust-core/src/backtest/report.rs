use super::engine::BacktestResult;

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
