//! Terminal dashboard for a running arbx-core engine.
//!
//! Polls the engine's admin HTTP `/status` (default `:9091`) AND the
//! Prometheus `/metrics` endpoint (default `:9090`) every poll_ms and
//! renders a five-pane layout: header, run-state counters, circuit-breaker,
//! WS connectivity (per-venue pub + priv), and last poll error.
//!
//! Usage:
//!   arbx_tui
//!   arbx_tui --admin-url http://host:9091 --metrics-url http://host:9090/metrics
//!
//! Keys: `q` / Esc quit, `p` pause/resume.

use std::collections::BTreeMap;
use std::io;
use std::time::{Duration, Instant};

use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    name = "arbx-tui",
    version,
    about = "Terminal dashboard for a running arbx-core engine"
)]
struct Cli {
    /// Base URL of the engine's admin HTTP endpoint.
    #[arg(long, default_value = "http://127.0.0.1:9091")]
    admin_url: String,
    /// Full URL of the engine's Prometheus metrics endpoint.
    #[arg(long, default_value = "http://127.0.0.1:9090/metrics")]
    metrics_url: String,
    /// Polling interval in milliseconds.
    #[arg(long, default_value_t = 500)]
    poll_ms: u64,
    /// Bearer token for the engine's admin HTTP endpoint. Required for
    /// /pause and /resume when the engine is started with ARBX_ADMIN_TOKEN.
    /// Falls back to env `ARBX_ADMIN_TOKEN` when this flag is absent.
    #[arg(long)]
    bearer_token: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct StatusBody {
    paused: bool,
    trade_log_count: usize,
    position_count: usize,
    cb_tripped: bool,
    cb_reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct WsStatus {
    /// venue → connected (1.0 = up, 0.0 = down). BTreeMap so display ordering
    /// is stable across polls.
    public: BTreeMap<String, bool>,
    private: BTreeMap<String, bool>,
}

#[derive(Debug, Clone, Default)]
struct AppState {
    last_status: Option<StatusBody>,
    last_ws: WsStatus,
    last_poll_at: Option<Instant>,
    /// Tracks when the last *successful* /status poll completed. Used to show
    /// a [STALE] indicator when the engine becomes unreachable.
    last_successful_poll: Option<Instant>,
    last_error: Option<String>,
}

/// Parse a Prometheus exposition body looking for a single gauge metric with a
/// `venue="..."` label. Returns (venue → value) for matching lines. Comments
/// (`#`) and lines that don't match the metric name are skipped.
fn parse_venue_gauge(text: &str, metric: &str) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if !line.starts_with(metric) {
            continue;
        }
        let Some(open) = line.find('{') else { continue };
        let Some(close) = line.find('}') else {
            continue;
        };
        let labels = &line[open + 1..close];
        let value_str = line[close + 1..].trim();
        let venue = labels.split(',').find_map(|kv| {
            let mut parts = kv.splitn(2, '=');
            let k = parts.next()?.trim();
            let v = parts.next()?.trim().trim_matches('"');
            (k == "venue").then(|| v.to_string())
        });
        if let (Some(v), Ok(n)) = (venue, value_str.parse::<f64>()) {
            out.insert(v, n);
        }
    }
    out
}

async fn poll_metrics(client: &reqwest::Client, url: &str) -> Result<WsStatus, String> {
    let resp = client
        .get(url)
        .timeout(Duration::from_millis(750))
        .send()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("GET {url}: status {}", resp.status()));
    }
    let body = resp.text().await.map_err(|e| format!("read {url}: {e}"))?;
    let public = parse_venue_gauge(&body, "arbx_ws_connected")
        .into_iter()
        .map(|(k, v)| (k, v >= 1.0))
        .collect();
    let private = parse_venue_gauge(&body, "arbx_ws_private_connected")
        .into_iter()
        .map(|(k, v)| (k, v >= 1.0))
        .collect();
    Ok(WsStatus { public, private })
}

async fn poll_status(client: &reqwest::Client, base: &str) -> Result<StatusBody, String> {
    let url = format!("{}/status", base.trim_end_matches('/'));
    let resp = client
        .get(&url)
        .timeout(Duration::from_millis(750))
        .send()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("GET {url}: status {}", resp.status()));
    }
    resp.json::<StatusBody>()
        .await
        .map_err(|e| format!("decode {url}: {e}"))
}

async fn post_pause_resume(
    client: &reqwest::Client,
    base: &str,
    target_paused: bool,
    bearer: Option<&str>,
) -> Result<(), String> {
    let path = if target_paused { "/pause" } else { "/resume" };
    let url = format!("{}{}", base.trim_end_matches('/'), path);
    let mut req = client.post(&url).timeout(Duration::from_millis(750));
    if let Some(token) = bearer {
        req = req.bearer_auth(token);
    }
    let resp = req.send().await.map_err(|e| format!("POST {url}: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("POST {url}: status {}", resp.status()));
    }
    Ok(())
}

fn render(frame: &mut ratatui::Frame, state: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Length(8),
            Constraint::Min(1),
            Constraint::Length(2),
        ])
        .split(frame.area());

    let is_stale = state
        .last_successful_poll
        .is_some_and(|t| t.elapsed() > Duration::from_secs(3));

    let header_text = match &state.last_status {
        Some(s) if s.paused => Span::styled(
            "ArbX — PAUSED",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Some(_) => Span::styled(
            "ArbX — running",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        None => Span::styled(
            "ArbX — connecting…",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        ),
    };

    let header_spans = if is_stale {
        vec![
            header_text,
            Span::raw("  "),
            Span::styled(
                "[STALE]",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            ),
        ]
    } else {
        vec![header_text]
    };
    frame.render_widget(
        Paragraph::new(Line::from(header_spans)).block(Block::default().borders(Borders::ALL)),
        chunks[0],
    );

    let status_text = match &state.last_status {
        Some(s) => format!(
            "trade_log_count: {}\nposition_count:  {}",
            s.trade_log_count, s.position_count,
        ),
        None => "(no data)".to_string(),
    };
    frame.render_widget(
        Paragraph::new(status_text)
            .block(Block::default().borders(Borders::ALL).title("Run state")),
        chunks[1],
    );

    let cb_text = match &state.last_status {
        Some(s) if s.cb_tripped => format!(
            "TRIPPED\nreason: {}",
            s.cb_reason.as_deref().unwrap_or("(unknown)")
        ),
        Some(_) => "armed".to_string(),
        None => "(no data)".to_string(),
    };
    let cb_style = match &state.last_status {
        Some(s) if s.cb_tripped => Style::default().fg(Color::Red),
        _ => Style::default(),
    };
    frame.render_widget(
        Paragraph::new(cb_text).style(cb_style).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Circuit breaker"),
        ),
        chunks[2],
    );

    let ws_text = if state.last_ws.public.is_empty() && state.last_ws.private.is_empty() {
        "(no WS gauges seen yet — check --metrics-url)".to_string()
    } else {
        let mut venues: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
        venues.extend(state.last_ws.public.keys().map(String::as_str));
        venues.extend(state.last_ws.private.keys().map(String::as_str));
        venues
            .into_iter()
            .map(|v| {
                let pub_ = state.last_ws.public.get(v).copied().unwrap_or(false);
                let priv_ = state.last_ws.private.get(v).copied().unwrap_or(false);
                let mark = |b: bool| if b { "✓" } else { "✗" };
                format!("{:<10}  pub {}    priv {}", v, mark(pub_), mark(priv_))
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    frame.render_widget(
        Paragraph::new(ws_text).block(
            Block::default()
                .borders(Borders::ALL)
                .title("WS connectivity"),
        ),
        chunks[3],
    );

    let err_text = state
        .last_error
        .as_deref()
        .unwrap_or("(no errors observed)");
    frame.render_widget(
        Paragraph::new(err_text)
            .wrap(Wrap { trim: true })
            .style(Style::default().fg(Color::DarkGray))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Last poll error"),
            ),
        chunks[4],
    );

    frame.render_widget(
        Paragraph::new("[q] quit   [p] pause/resume").style(Style::default().fg(Color::DarkGray)),
        chunks[5],
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = reqwest::Client::new();
    let bearer = cli
        .bearer_token
        .clone()
        .or_else(|| std::env::var("ARBX_ADMIN_TOKEN").ok());

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut state = AppState::default();
    let mut last_poll: Option<Instant> = None;
    let poll_interval = Duration::from_millis(cli.poll_ms.max(50));

    let result: anyhow::Result<()> = loop {
        let now = Instant::now();
        if last_poll.is_none_or(|t| now.duration_since(t) >= poll_interval) {
            // /status is critical (header / counters); /metrics is best-effort.
            // Poll both each tick; metrics failure doesn't blank the rest.
            match poll_status(&client, &cli.admin_url).await {
                Ok(body) => {
                    state.last_status = Some(body);
                    state.last_successful_poll = Some(now);
                    state.last_error = None;
                }
                Err(e) => {
                    state.last_error = Some(e);
                }
            }
            match poll_metrics(&client, &cli.metrics_url).await {
                Ok(ws) => state.last_ws = ws,
                Err(e) => {
                    // Append to whatever /status set; don't overwrite the more
                    // critical error.
                    let combined = match state.last_error.take() {
                        Some(prev) => format!("{prev}\n{e}"),
                        None => e,
                    };
                    state.last_error = Some(combined);
                }
            }
            state.last_poll_at = Some(now);
            last_poll = Some(now);
        }

        if let Err(e) = terminal.draw(|f| render(f, &state)) {
            break Err(e.into());
        }

        if event::poll(Duration::from_millis(50))?
            && let Event::Key(k) = event::read()?
            && k.kind == KeyEventKind::Press
        {
            match k.code {
                KeyCode::Char('q') | KeyCode::Esc => break Ok(()),
                KeyCode::Char('p') => {
                    let target = !state.last_status.as_ref().is_some_and(|s| s.paused);
                    match post_pause_resume(&client, &cli.admin_url, target, bearer.as_deref())
                        .await
                    {
                        Ok(()) => {
                            // Force an immediate re-poll so the header flips
                            // on the very next frame instead of waiting up to
                            // poll_interval for /status to refresh.
                            last_poll = None;
                        }
                        Err(e) => state.last_error = Some(e),
                    }
                }
                _ => {}
            }
        }
    };

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}
