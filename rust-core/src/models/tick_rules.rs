use rust_decimal::Decimal;
use rust_decimal_macros::dec;

pub const MAX_TW_STOCK_PRICE: Decimal = dec!(99_999);

pub fn tw_stock_tick_aligned(price: Decimal) -> bool {
    let tick = tw_stock_tick_size(price);
    (price % tick).is_zero()
}

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

    #[test]
    fn tw_stock_tick_boundary_50() {
        assert_eq!(tw_stock_tick_size(dec!(50)), dec!(0.10));
    }

    #[test]
    fn tw_stock_tick_boundary_100() {
        assert_eq!(tw_stock_tick_size(dec!(100)), dec!(0.50));
    }

    #[test]
    fn tw_stock_tick_boundary_500() {
        assert_eq!(tw_stock_tick_size(dec!(500)), dec!(1.00));
    }

    #[test]
    fn tw_stock_tick_boundary_1000() {
        assert_eq!(tw_stock_tick_size(dec!(1000)), dec!(5.00));
    }

    #[test]
    fn tw_stock_tick_aligned_at_boundary() {
        assert!(tw_stock_tick_aligned(dec!(10.00)));
        assert!(tw_stock_tick_aligned(dec!(50.0)));
        assert!(tw_stock_tick_aligned(dec!(100.0)));
        assert!(tw_stock_tick_aligned(dec!(500.0)));
        assert!(tw_stock_tick_aligned(dec!(1000)));
    }

    #[test]
    fn tw_stock_tick_aligned_valid_prices() {
        assert!(tw_stock_tick_aligned(dec!(5.01)));
        assert!(tw_stock_tick_aligned(dec!(25.05)));
        assert!(tw_stock_tick_aligned(dec!(75.10)));
        assert!(tw_stock_tick_aligned(dec!(250.50)));
        assert!(tw_stock_tick_aligned(dec!(750)));
        assert!(tw_stock_tick_aligned(dec!(1500)));
    }

    #[test]
    fn tw_stock_tick_aligned_invalid_prices() {
        // below 10: tick = 0.01, so 5.001 is misaligned
        assert!(!tw_stock_tick_aligned(dec!(5.001)));
        // 10-50: tick = 0.05, so 25.01 is misaligned
        assert!(!tw_stock_tick_aligned(dec!(25.01)));
        // 50-100: tick = 0.10, so 75.05 is misaligned
        assert!(!tw_stock_tick_aligned(dec!(75.05)));
        // 100-500: tick = 0.50, so 200.10 is misaligned
        assert!(!tw_stock_tick_aligned(dec!(200.10)));
        // 500-1000: tick = 1.00, so 750.50 is misaligned
        assert!(!tw_stock_tick_aligned(dec!(750.50)));
        // above 1000: tick = 5.00, so 1501 is misaligned
        assert!(!tw_stock_tick_aligned(dec!(1501)));
    }

    #[test]
    fn max_tw_stock_price_constant() {
        assert_eq!(MAX_TW_STOCK_PRICE, dec!(99_999));
    }
}
