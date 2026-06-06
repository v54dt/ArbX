# ArbX notifier

Central notification service for the ArbX monorepo. ArbX components (tw-exec, and
later rust-core / python-sidecar) POST notification events here over HTTP; the
notifier forwards them to the upstream ntfy server. Having one process own all
notification traffic lets us add cross-source de-dup, throttling, the ntfy.sh
250-messages/12h budget, central on/off filtering and a retry queue in one place.

## Build

```bash
cd notifier
cmake -S . -B build
cmake --build build -j
```

(First build fetches cpp-httplib + tomlplusplus via CMake FetchContent.)

## Run

```bash
cp notifier.toml.example notifier.toml   # fill in [ntfy] token
./build/notifier notifier.toml
```

## API

| Method | Path | Body | Effect |
|--------|------|------|--------|
| `POST` | `/` | ntfy JSON `{topic,title,message,priority,tags}` | forwarded to ntfy |
| `GET`  | `/healthz` | — | `{"healthy":true}` |

Smoke test:

```bash
curl -s localhost:8095/healthz
curl -s -X POST localhost:8095/ -H 'Content-Type: application/json' \
  -d '{"topic":"arbx-trade","title":"notifier test","message":"hello","priority":3,"tags":["white_check_mark"]}'
```

## Roadmap (PR by PR)

1. **Extract** ✅ — standalone service, ntfy-format passthrough (this PR).
2. Point tw-exec at the notifier (`[notify] base_url` → `http://127.0.0.1:8095`).
3. Cross-source de-dup + throttle.
4. Central filtering / per-class on-off (move `on_fills` / `on_quote_stall` … here).
5. ntfy.sh 250/12h budget management (degrade / drop low-priority near the cap).
6. Reliability: retry queue, survive restart.
