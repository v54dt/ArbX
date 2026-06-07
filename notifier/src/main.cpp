// ArbX notifier — central notification service.
//
// PR 2: structured events + exact de-dup + /stats. Incoming ntfy-format POSTs are
// parsed into events; an identical (topic+title+message) event seen again within
// the de-dup window is dropped instead of forwarded. /stats exposes counters so
// the behaviour is observable and CI-testable. Unparseable bodies are forwarded
// as-is (never silently lost).
//
//   Usage: notifier [notifier.toml]
//   Endpoints:
//     POST /         ntfy-format JSON {topic,title,message,priority,tags} -> ntfy
//     GET  /healthz  liveness
//     GET  /stats    {received,forwarded,deduped,forward_failed}

#include <curl/curl.h>
#include <httplib.h>
#include <nlohmann/json.hpp>
#include <toml++/toml.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <deque>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {
namespace chrono = std::chrono;
using json = nlohmann::json;

httplib::Server* g_server = nullptr;
void OnSig(int) {
  if (g_server) g_server->stop();
}

size_t DiscardBody(char*, size_t size, size_t nmemb, void*) { return size * nmemb; }

struct Stats {
  std::atomic<long> received{0};
  std::atomic<long> forwarded{0};
  std::atomic<long> deduped{0};
  std::atomic<long> filtered{0};
  std::atomic<long> budget_dropped{0};
  std::atomic<long> retried_ok{0};
};

// Exact de-dup: drop an event whose key was seen within `window`.
class Dedup {
 public:
  explicit Dedup(int window_s) : window_(window_s) {}
  bool seen(const std::string& key) {
    if (window_.count() <= 0) return false;  // disabled
    auto now = chrono::steady_clock::now();
    std::lock_guard<std::mutex> lk(mu_);
    if (last_.size() > 4096) {  // opportunistic prune so the map can't grow unbounded
      for (auto it = last_.begin(); it != last_.end();)
        it = (now - it->second) > window_ ? last_.erase(it) : std::next(it);
    }
    auto it = last_.find(key);
    if (it != last_.end() && (now - it->second) < window_) {
      it->second = now;  // refresh -> a steady stream stays suppressed
      return true;
    }
    last_[key] = now;
    return false;
  }

 private:
  chrono::seconds window_;
  std::mutex mu_;
  std::unordered_map<std::string, chrono::steady_clock::time_point> last_;
};

// Rolling-window send budget. Caps forwards within `window` to `max` (the ntfy.sh
// free tier is ~250 messages / 12h per IP). Events with priority >= `reserve`
// always pass, so critical alerts are never budget-dropped.
class Budget {
 public:
  Budget(int max, int window_s, int reserve) : max_(max), window_(window_s), reserve_(reserve) {}
  // Records and allows a forward, or returns false if the budget is exhausted.
  bool allow(int prio) {
    if (max_ <= 0) return true;  // disabled
    auto now = chrono::steady_clock::now();
    std::lock_guard<std::mutex> lk(mu_);
    Prune(now);
    if (static_cast<long>(times_.size()) >= max_ && !(reserve_ > 0 && prio >= reserve_))
      return false;  // exhausted and not a reserved (high) priority
    times_.push_back(now);
    return true;
  }
  long used() {
    auto now = chrono::steady_clock::now();
    std::lock_guard<std::mutex> lk(mu_);
    Prune(now);
    return static_cast<long>(times_.size());
  }

 private:
  void Prune(chrono::steady_clock::time_point now) {
    while (!times_.empty() && (now - times_.front()) > window_) times_.pop_front();
  }
  long max_;
  chrono::seconds window_;
  int reserve_;
  std::mutex mu_;
  std::deque<chrono::steady_clock::time_point> times_;
};

bool ForwardToNtfy(const std::string& base_url, const std::string& token,
                   const std::string& body) {
  CURL* c = curl_easy_init();
  if (!c) return false;
  struct curl_slist* hdrs = nullptr;
  hdrs = curl_slist_append(hdrs, "Content-Type: application/json");
  std::string auth;
  if (!token.empty()) {
    auth = "Authorization: Bearer " + token;
    hdrs = curl_slist_append(hdrs, auth.c_str());
  }
  curl_easy_setopt(c, CURLOPT_URL, base_url.c_str());
  curl_easy_setopt(c, CURLOPT_POSTFIELDS, body.c_str());
  curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
  curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);
  curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, DiscardBody);
  curl_easy_setopt(c, CURLOPT_TIMEOUT, 10L);
  CURLcode rc = curl_easy_perform(c);
  long http_code = 0;
  curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &http_code);
  curl_slist_free_all(hdrs);
  curl_easy_cleanup(c);
  if (rc != CURLE_OK) {
    std::cerr << "forward failed: " << curl_easy_strerror(rc) << std::endl;
    return false;
  }
  if (http_code < 200 || http_code >= 300) {
    std::cerr << "forward got HTTP " << http_code << std::endl;
    return false;
  }
  return true;
}

struct RetryItem {
  std::string body;
  int attempts = 0;
  chrono::steady_clock::time_point next_try;
};

// Retry queue for failed forwards: a background worker retries with exponential
// backoff up to max_attempts, then drops. A transient ntfy outage no longer loses
// notifications. Budget/dedup/filter already decided to send, so retries do NOT
// re-consume the budget.
class RetryQueue {
 public:
  RetryQueue(int max_attempts, int base_s, size_t max_queue)
      : max_attempts_(max_attempts), base_(base_s), max_queue_(max_queue) {}

  void push(std::string body) {
    auto now = chrono::steady_clock::now();
    std::lock_guard<std::mutex> lk(mu_);
    if (q_.size() >= max_queue_) {
      dropped_.fetch_add(1, std::memory_order_relaxed);  // queue overflow
      return;
    }
    q_.push_back({std::move(body), 0, now + base_});
  }

  // Retry every due item once; reschedule failures with backoff, drop at the cap.
  void tick(const std::string& base_url, const std::string& token,
            std::atomic<long>& retried_ok) {
    auto now = chrono::steady_clock::now();
    std::deque<RetryItem> due;
    {
      std::lock_guard<std::mutex> lk(mu_);
      for (auto it = q_.begin(); it != q_.end();) {
        if (it->next_try <= now) {
          due.push_back(std::move(*it));
          it = q_.erase(it);
        } else {
          ++it;
        }
      }
    }
    for (auto& item : due) {
      if (ForwardToNtfy(base_url, token, item.body)) {
        retried_ok.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      if (++item.attempts >= max_attempts_) {
        dropped_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      int shift = item.attempts < 16 ? item.attempts : 16;  // cap backoff growth
      item.next_try = now + base_ * (1 << shift);
      std::lock_guard<std::mutex> lk(mu_);
      q_.push_back(std::move(item));
    }
  }

  long size() {
    std::lock_guard<std::mutex> lk(mu_);
    return static_cast<long>(q_.size());
  }
  long dropped() const { return dropped_.load(std::memory_order_relaxed); }

 private:
  int max_attempts_;
  chrono::seconds base_;
  size_t max_queue_;
  std::mutex mu_;
  std::deque<RetryItem> q_;
  std::atomic<long> dropped_{0};
};

}  // namespace

int main(int argc, char** argv) {
  std::string cfg_path = argc > 1 ? argv[1] : "notifier.toml";
  std::string host = "127.0.0.1";
  int port = 8095;
  std::string ntfy_base, ntfy_token;
  int dedup_window_s = 30;
  int min_priority = 1;             // drop events with priority < this (1 = forward all)
  std::vector<std::string> mute_titles;  // drop events whose title starts with any of these
  int budget_max = 250;             // max forwards per window (ntfy.sh free ~250/12h)
  int budget_window_s = 43200;      // 12h
  int budget_reserve = 5;           // priority >= this always passes (never budget-dropped)
  int retry_max_attempts = 5;       // retry a failed forward this many times before dropping
  int retry_base_s = 5;             // first retry after this many seconds (then exp backoff)
  int retry_max_queue = 1000;       // cap the retry queue
  try {
    auto t = toml::parse_file(cfg_path);
    host = t["listen"]["host"].value_or<std::string>("127.0.0.1");
    port = t["listen"]["port"].value_or<int>(8095);
    ntfy_base = t["ntfy"]["base_url"].value_or<std::string>("");
    ntfy_token = t["ntfy"]["token"].value_or<std::string>("");
    dedup_window_s = t["policy"]["dedup_window_s"].value_or<int>(30);
    min_priority = t["filter"]["min_priority"].value_or<int>(1);
    if (auto arr = t["filter"]["mute_titles"].as_array()) {
      for (auto& el : *arr)
        if (auto s = el.value<std::string>()) mute_titles.push_back(*s);
    }
    budget_max = t["budget"]["max_per_window"].value_or<int>(250);
    budget_window_s = t["budget"]["window_s"].value_or<int>(43200);
    budget_reserve = t["budget"]["reserve_priority"].value_or<int>(5);
    retry_max_attempts = t["reliability"]["max_attempts"].value_or<int>(5);
    retry_base_s = t["reliability"]["base_s"].value_or<int>(5);
    retry_max_queue = t["reliability"]["max_queue"].value_or<int>(1000);
  } catch (const std::exception& e) {
    std::cerr << "failed to parse " << cfg_path << ": " << e.what() << std::endl;
    return 1;
  }
  if (ntfy_base.empty()) {
    std::cerr << "notifier: [ntfy] base_url is required\n";
    return 1;
  }

  curl_global_init(CURL_GLOBAL_DEFAULT);

  Stats stats;
  Dedup dedup(dedup_window_s);
  Budget budget(budget_max, budget_window_s, budget_reserve);
  RetryQueue retryq(retry_max_attempts, retry_base_s, static_cast<size_t>(retry_max_queue));

  httplib::Server svr;
  g_server = &svr;
  std::signal(SIGINT, OnSig);
  std::signal(SIGTERM, OnSig);

  svr.Get("/healthz", [](const httplib::Request&, httplib::Response& res) {
    res.set_content("{\"healthy\":true}", "application/json");
  });

  svr.Get("/stats", [&](const httplib::Request&, httplib::Response& res) {
    json j = {{"received", stats.received.load()},
              {"forwarded", stats.forwarded.load()},
              {"deduped", stats.deduped.load()},
              {"filtered", stats.filtered.load()},
              {"budget_used", budget.used()},
              {"budget_dropped", stats.budget_dropped.load()},
              {"retried_ok", stats.retried_ok.load()},
              {"retry_queue", retryq.size()},
              {"retry_dropped", retryq.dropped()}};
    res.set_content(j.dump(), "application/json");
  });

  svr.Post("/", [&](const httplib::Request& req, httplib::Response& res) {
    stats.received.fetch_add(1, std::memory_order_relaxed);

    std::string key, title;
    int prio = 3;
    try {
      auto j = json::parse(req.body);
      title = j.value("title", "");
      prio = j.value("priority", 3);
      key = j.value("topic", "") + "\x1f" + title + "\x1f" + j.value("message", "");
    } catch (...) {
      // Unparseable body -> forward as-is, never silently drop.
      if (ForwardToNtfy(ntfy_base, ntfy_token, req.body))
        stats.forwarded.fetch_add(1, std::memory_order_relaxed);
      else
        retryq.push(req.body);
      res.status = 200;
      res.set_content("{\"ok\":true}", "application/json");
      return;
    }

    // Central filter: drop below the priority floor, or titles muted by config.
    bool muted = prio < min_priority;
    if (!muted)
      for (const auto& m : mute_titles)
        if (!m.empty() && title.rfind(m, 0) == 0) {
          muted = true;
          break;
        }
    if (muted) {
      stats.filtered.fetch_add(1, std::memory_order_relaxed);
      res.status = 200;
      res.set_content("{\"filtered\":true}", "application/json");
      return;
    }

    if (dedup.seen(key)) {
      stats.deduped.fetch_add(1, std::memory_order_relaxed);
      res.status = 200;
      res.set_content("{\"deduped\":true}", "application/json");
      return;
    }

    if (!budget.allow(prio)) {
      stats.budget_dropped.fetch_add(1, std::memory_order_relaxed);
      res.status = 200;
      res.set_content("{\"budget_dropped\":true}", "application/json");
      return;
    }

    if (ForwardToNtfy(ntfy_base, ntfy_token, req.body))
      stats.forwarded.fetch_add(1, std::memory_order_relaxed);
    else
      retryq.push(req.body);  // transient failure -> retry in the background
    res.status = 200;
    res.set_content("{\"ok\":true}", "application/json");
  });

  std::cout << "ArbX notifier listening on " << host << ":" << port << " -> " << ntfy_base
            << " (dedup " << dedup_window_s << "s, min_priority " << min_priority
            << ", mute_titles " << mute_titles.size() << ", budget " << budget_max << "/"
            << budget_window_s << "s reserve>=" << budget_reserve << ", retry "
            << retry_max_attempts << "x/" << retry_base_s << "s)" << std::endl;

  std::atomic<bool> worker_stop{false};
  std::thread worker([&] {
    while (!worker_stop.load()) {
      retryq.tick(ntfy_base, ntfy_token, stats.retried_ok);
      std::this_thread::sleep_for(chrono::milliseconds(500));
    }
  });

  bool bound = svr.listen(host, port);  // blocks until stop()
  worker_stop = true;
  worker.join();
  if (!bound) {
    std::cerr << "notifier: failed to bind " << host << ":" << port << std::endl;
    curl_global_cleanup();
    return 1;
  }
  std::cout << "ArbX notifier stopped" << std::endl;
  curl_global_cleanup();
  return 0;
}
