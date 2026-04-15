//! Thin Aeron publisher CLI for the Python sidecar. Reads length-prefixed
//! byte frames from stdin and publishes each to a given Aeron stream id.
//!
//! Wire format on stdin:
//!     [ 4-byte length BE ][ N bytes payload ]... repeated
//!
//! The payload is written to Aeron verbatim — e.g. the Python sidecar
//! prepends its own 1-byte MSG_TAG_* before the FlatBuffers bytes, and
//! we forward the whole thing. The subscriber side (AeronMarketDataFeed)
//! handles the tag demux.
//!
//! Exits on stdin EOF (graceful) or on first publish error (non-zero).

use std::io::Read;

use anyhow::Context as _;
use arbx_core::ipc::IpcPublisher;
use arbx_core::ipc::aeron::AeronPublisher;
use clap::Parser;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(
    name = "arbx-aeron-pub",
    about = "Bridge: stdin length-prefixed frames → Aeron publication"
)]
struct Cli {
    /// Aeron stream id to publish to. Must match the subscriber side.
    #[arg(long, default_value_t = 1001)]
    stream_id: i32,

    /// Log filter (RUST_LOG semantics). Default: info.
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cli.log_level));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    info!(stream_id = cli.stream_id, "arbx-aeron-pub starting");
    let publisher =
        AeronPublisher::new(cli.stream_id).context("failed to create AeronPublisher")?;

    let mut stdin = std::io::stdin().lock();
    let mut len_buf = [0u8; 4];
    let mut frame_count: u64 = 0;

    loop {
        match stdin.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                info!(frames_sent = frame_count, "stdin EOF, exiting");
                return Ok(());
            }
            Err(e) => return Err(e).context("reading length prefix"),
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 {
            warn!("zero-length frame, skipping");
            continue;
        }
        let mut payload = vec![0u8; len];
        stdin
            .read_exact(&mut payload)
            .with_context(|| format!("reading {}-byte payload", len))?;

        publisher
            .publish(&payload)
            .await
            .with_context(|| format!("publish frame {}", frame_count))?;
        frame_count += 1;
    }
}
