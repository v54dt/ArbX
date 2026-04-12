use rust_decimal::Decimal;
use rust_decimal_macros::dec;

pub fn tw_stock_tick_size(price: Decimal) -> Decimal {
    if price < dec!(10) {
        dec!(0.01)
    } else if price < dec!(50) {
        dec!(0.05)
    } else if price < dec!(100) {
        dec!(0.10)
    } else if price < dec!(500) {
        dec!(0.50)
    } else if price < dec!(1000) {
        dec!(1.00)
    } else {
        dec!(5.00)
    }
}

pub fn tw_futures_tick_size() -> Decimal {
    dec!(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tw_stock_tick_below_10() {
        assert_eq!(tw_stock_tick_size(dec!(5)), dec!(0.01));
    }

    #[test]
    fn tw_stock_tick_10_to_50() {
        assert_eq!(tw_stock_tick_size(dec!(25)), dec!(0.05));
    }

    #[test]
    fn tw_stock_tick_100_to_500() {
        assert_eq!(tw_stock_tick_size(dec!(200)), dec!(0.50));
    }

    #[test]
    fn tw_stock_tick_above_1000() {
        assert_eq!(tw_stock_tick_size(dec!(1500)), dec!(5.00));
    }

    #[test]
    fn tw_stock_tick_boundary_10() {
        assert_eq!(tw_stock_tick_size(dec!(10)), dec!(0.05));
    }
}
