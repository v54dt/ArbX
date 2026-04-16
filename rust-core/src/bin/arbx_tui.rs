//! Terminal dashboard for a running arbx-core engine.
//!
//! Polls the engine's admin HTTP `/status` endpoint (default :9091) every
//! 500 ms and renders a four-pane layout: header, run-state counters,
//! circuit-breaker, and a hint footer. `q` quits.
//!
//! Usage:
//!   arbx_tui                       (uses http://127.0.0.1:9091)
//!   arbx_tui --admin-url http://host:9091
//!
//! Subsequent PRs add per-venue WS status, opportunity feed, position table,
//! and `p` pause / resume keybinding.

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
    /// Polling interval in milliseconds.
    #[arg(long, default_value_t = 500)]
    poll_ms: u64,
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
struct AppState {
    last_status: Option<StatusBody>,
    last_poll_at: Option<Instant>,
    last_error: Option<String>,
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

fn render(frame: &mut ratatui::Frame, state: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Min(1),
            Constraint::Length(2),
        ])
        .split(frame.area());

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
    frame.render_widget(
        Paragraph::new(Line::from(header_text)).block(Block::default().borders(Borders::ALL)),
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
        chunks[3],
    );

    frame.render_widget(
        Paragraph::new("[q] quit").style(Style::default().fg(Color::DarkGray)),
        chunks[4],
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = reqwest::Client::new();

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
            match poll_status(&client, &cli.admin_url).await {
                Ok(body) => {
                    state.last_status = Some(body);
                    state.last_error = None;
                }
                Err(e) => {
                    state.last_error = Some(e);
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
            && matches!(k.code, KeyCode::Char('q') | KeyCode::Esc)
        {
            break Ok(());
        }
    };

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}
