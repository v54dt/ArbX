// ArbX notifier — central notification service.
//
//
//   Usage: notifier [notifier.toml]
//   Endpoints:
//     POST /         ntfy-format JSON body {topic,title,message,priority,tags}
//     -> ntfy GET  /healthz  liveness

#include <curl/curl.h>
#include <httplib.h>
#include <toml++/toml.h>

#include <atomic>
#include <csignal>
#include <iostream>
#include <string>

namespace {

httplib::Server* g_server = nullptr;
void OnSig(int) {
  if (g_server) g_server->stop();
}

size_t DiscardBody(char*, size_t size, size_t nmemb, void*) {
  return size * nmemb;
}

// Forward a raw ntfy JSON body to the upstream ntfy server. Best-effort; a
// failure is logged but never blocks the caller for long (10s cap).
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

}  // namespace

int main(int argc, char** argv) {
  std::string cfg_path = argc > 1 ? argv[1] : "notifier.toml";
  std::string host = "127.0.0.1";
  int port = 8095;
  std::string ntfy_base, ntfy_token;
  try {
    auto t = toml::parse_file(cfg_path);
    host = t["listen"]["host"].value_or<std::string>("127.0.0.1");
    port = t["listen"]["port"].value_or<int>(8095);
    ntfy_base = t["ntfy"]["base_url"].value_or<std::string>("");
    ntfy_token = t["ntfy"]["token"].value_or<std::string>("");
  } catch (const std::exception& e) {
    std::cerr << "failed to parse " << cfg_path << ": " << e.what()
              << std::endl;
    return 1;
  }
  if (ntfy_base.empty()) {
    std::cerr << "notifier: [ntfy] base_url is required\n";
    return 1;
  }

  curl_global_init(CURL_GLOBAL_DEFAULT);

  httplib::Server svr;
  g_server = &svr;
  std::signal(SIGINT, OnSig);
  std::signal(SIGTERM, OnSig);

  svr.Get("/healthz", [](const httplib::Request&, httplib::Response& res) {
    res.set_content("{\"healthy\":true}", "application/json");
  });

  svr.Post("/", [&](const httplib::Request& req, httplib::Response& res) {
    bool ok = ForwardToNtfy(ntfy_base, ntfy_token, req.body);
    res.status = ok ? 200 : 502;
    res.set_content(ok ? "{\"ok\":true}" : "{\"ok\":false}",
                    "application/json");
  });

  std::cout << "ArbX notifier listening on " << host << ":" << port << " -> "
            << ntfy_base << std::endl;
  if (!svr.listen(host, port)) {  // blocks until stop()
    std::cerr << "notifier: failed to bind " << host << ":" << port
              << std::endl;
    curl_global_cleanup();
    return 1;
  }
  std::cout << "ArbX notifier stopped" << std::endl;
  curl_global_cleanup();
  return 0;
}
