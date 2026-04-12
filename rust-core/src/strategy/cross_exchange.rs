use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use smallvec::smallvec;

use crate::models::enums::{OrderType, Side, Venue};
use crate::models::instrument::Instrument;
use crate::models::market::{OrderBook, book_key};
use crate::models::order::Order;
use crate::models::position::PortfolioSnapshot;

use super::base::ArbitrageStrategy;
use super::{Economics, Leg, Opportunity, OpportunityKind, OpportunityMeta};

/// Cross-exchange spot arbitrage between two venues.
/// TODO: walk-the-book VWAP, staleness rejection
pub struct CrossExchangeStrategy {
    pub venue_a: Venue,
    pub venue_b: Venue,
    pub instrument_a: Instrument,
    pub instrument_b: Instrument,
    pub min_net_profit_bps: Decimal,
    pub max_quantity: Decimal,
    pub fee_rate_a: Decimal,
    pub fee_rate_b: Decimal,
}

struct DirectionParams<'a> {
    buy_venue: Venue,
    buy_instrument: &'a Instrument,
    sell_venue: Venue,
    sell_instrument: &'a Instrument,
    buy_fee: Decimal,
    sell_fee: Decimal,
}

impl CrossExchangeStrategy {
    /// Returns None if no profitable trade exists in this direction.
    fn evaluate_direction(
        &self,
        books: &HashMap<String, OrderBook>,
        params: DirectionParams<'_>,
    ) -> Option<Opportunity> {
        let DirectionParams {
            buy_venue,
            buy_instrument,
            sell_venue,
            sell_instrument,
            buy_fee,
            sell_fee,
        } = params;
        let buy_book = books.get(&book_key(buy_venue, buy_instrument))?;
        let sell_book = books.get(&book_key(sell_venue, sell_instrument))?;

        let best_ask = buy_book.best_ask()?;
        let best_bid = sell_book.best_bid()?;

        if best_bid.price <= best_ask.price {
            return None;
        }

        let quantity = best_ask.size.min(best_bid.size).min(self.max_quantity);
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
                instrument: buy_instrument.clone(),
                side: Side::Buy,
                quote_price: best_ask.price,
                order_price: best_ask.price,
                quantity,
                fee_estimate: fee_buy,
            },
            Leg {
                venue: sell_venue,
                instrument: sell_instrument.clone(),
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
        let a_to_b = self.evaluate_direction(
            books,
            DirectionParams {
                buy_venue: self.venue_a,
                buy_instrument: &self.instrument_a,
                sell_venue: self.venue_b,
                sell_instrument: &self.instrument_b,
                buy_fee: self.fee_rate_a,
                sell_fee: self.fee_rate_b,
            },
        );
        let b_to_a = self.evaluate_direction(
            books,
            DirectionParams {
                buy_venue: self.venue_b,
                buy_instrument: &self.instrument_b,
                sell_venue: self.venue_a,
                sell_instrument: &self.instrument_a,
                buy_fee: self.fee_rate_b,
                sell_fee: self.fee_rate_a,
            },
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
                instrument: leg.instrument.clone(),
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
