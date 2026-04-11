use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use smallvec::smallvec;

use crate::models::enums::{OrderType, Side, Venue};
use crate::models::market::OrderBook;
use crate::models::order::Order;
use crate::models::position::PortfolioSnapshot;

use super::base::ArbitrageStrategy;
use super::{Economics, Leg, Opportunity, OpportunityKind, OpportunityMeta};

/// Cross-exchange spot arbitrage between two venues.
/// TODO: walk-the-book VWAP, staleness regection
pub struct CrossExchangeStrategy {
    pub venue_a: Venue,
    pub venue_b: Venue,
    pub symbol: String,
    /// Minimum NET profit (after fees) in basis points to trigger.
    pub min_net_profit_bps: Decimal,
    /// Maximum quantity to trade per opportunity.
    pub max_quantity: Decimal,
    /// Taker fee rate at venue A (e.g. 0.001 = 0.1%).
    pub fee_rate_a: Decimal,
    /// Taker fee rate at venue B.
    pub fee_rate_b: Decimal,
}

impl CrossExchangeStrategy {
    fn book_key(venue: Venue, symbol: &str) -> String {
        format!("{:?}:{}", venue, symbol).to_lowercase()
    }

    /// Returns None if no profitable trade exists in this direction.
    fn evaluate_direction(
        &self,
        books: &HashMap<String, OrderBook>,
        buy_venue: Venue,
        sell_venue: Venue,
        buy_fee: Decimal,
        sell_fee: Decimal,
    ) -> Option<Opportunity> {
        let buy_book = books.get(&Self::book_key(buy_venue, &self.symbol))?;
        let sell_book = books.get(&Self::book_key(sell_venue, &self.symbol))?;

        let best_ask = buy_book.best_ask()?;
        let best_bid = sell_book.best_bid()?;

        // No edge if sell price is not higher than buy price.
        if best_bid.price <= best_ask.price {
            return None;
        }

        // Quantity is limited by available depth on both legs and our cap.
        let quantity = best_ask
            .size
            .min(best_bid.size)
            .min(self.max_quantity);
        if quantity <= Decimal::ZERO {
            return None;
        }

        let gross_profit = (best_bid.price - best_ask.price) * quantity;

        let fee_buy = best_ask.price * quantity * buy_fee;
        let fee_sell = best_bid.price * quantity * sell_fee;
        let fees_total = fee_buy + fee_sell;

        let net_profit = gross_profit - fees_total;
        if net_profit <= Decimal::ZERO {
            return None;
        }

        let notional = best_ask.price * quantity;
        let net_profit_bps = (net_profit / notional) * Decimal::from(10_000);

        if net_profit_bps < self.min_net_profit_bps {
            return None;
        }

        let now = Utc::now();
        let legs = smallvec![
            Leg {
                venue: buy_venue,
                instrument: self.symbol.clone(),
                side: Side::Buy,
                quote_price: best_ask.price,
                order_price: best_ask.price,
                quantity,
                fee_estimate: fee_buy,
            },
            Leg {
                venue: sell_venue,
                instrument: self.symbol.clone(),
                side: Side::Sell,
                quote_price: best_bid.price,
                order_price: best_bid.price,
                quantity,
                fee_estimate: fee_sell,
            },
        ];

        Some(Opportunity {
            id: format!("{}-{}", self.name(), now.timestamp_nanos_opt().unwrap_or(0)),
            kind: OpportunityKind::CrossExchange,
            legs,
            economics: Economics {
                gross_profit,
                fees_total,
                net_profit,
                net_profit_bps,
                notional,
            },
            meta: OpportunityMeta {
                detected_at: now,
                quote_ts_per_leg: smallvec![buy_book.timestamp, sell_book.timestamp],
                ttl: Duration::milliseconds(50),
                strategy_id: self.name().to_string(),
            },
        })
    }
}

#[async_trait]
impl ArbitrageStrategy for CrossExchangeStrategy {
    async fn evaluate(
        &self,
        books: &HashMap<String, OrderBook>,
        _portfolios: &HashMap<String, PortfolioSnapshot>,
    ) -> Option<Opportunity> {
        // Try both directions, return the more profitable one.
        let a_to_b = self.evaluate_direction(
            books,
            self.venue_a,
            self.venue_b,
            self.fee_rate_a,
            self.fee_rate_b,
        );
        let b_to_a = self.evaluate_direction(
            books,
            self.venue_b,
            self.venue_a,
            self.fee_rate_b,
            self.fee_rate_a,
        );

        match (a_to_b, b_to_a) {
            (Some(x), Some(y)) => {
                if x.economics.net_profit >= y.economics.net_profit {
                    Some(x)
                } else {
                    Some(y)
                }
            }
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        }
    }

    fn compute_hedge_orders(&self, opp: &Opportunity) -> Vec<Order> {
        let now = Utc::now();
        opp.legs
            .iter()
            .map(|leg| Order {
                id: String::new(),
                venue: leg.venue,
                symbol: leg.instrument.clone(),
                side: leg.side,
                order_type: OrderType::Limit,
                time_in_force: None,
                price: Some(leg.order_price),
                quantity: leg.quantity,
                created_at: now,
            })
            .collect()
    }

    fn name(&self) -> &str {
        "cross_exchange"
    }
}
