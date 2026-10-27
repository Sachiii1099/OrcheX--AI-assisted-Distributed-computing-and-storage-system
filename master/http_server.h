// A deliberately small HTTP/1.1 server (POSIX sockets, one thread per request,
// "Connection: close") that serves the dashboard and the JSON API. Written by
// hand so the project keeps zero third-party dependencies.
// Supported: GET/POST/DELETE, query strings, form bodies, raw bodies with
// Content-Length (also "Expect: 100-continue"). Not supported: TLS, chunked
// request bodies, keep-alive. Binds to 127.0.0.1 unless told otherwise.
#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../common/util.h"

namespace nexus {

struct HttpRequest {
    std::string method, path;
    Params query;                         // ?a=1&b=2
    Params form;                          // application/x-www-form-urlencoded body
    Params pathParams;                    // from ":name" segments of the route
    std::map<std::string, std::string> headers;   // lower-cased names
    std::string body;                     // raw body

    // query first, then form body
    std::string param(const std::string& k, const std::string& def = "") const {
        auto q = query.find(k);
        if (q != query.end()) return q->second;
        auto f = form.find(k);
        return f != form.end() ? f->second : def;
    }
    long long paramInt(const std::string& k, long long def) const {
        std::string v = param(k);
        if (v.empty()) return def;
        try { size_t used = 0; long long x = std::stoll(v, &used); return used == v.size() ? x : def; }
        catch (...) { return def; }
    }
};

struct HttpResponse {
    int status = 200;
    std::string contentType = "application/json";
    std::string body;
    std::map<std::string, std::string> headers;

    static HttpResponse json(const std::string& body, int status = 200) {
        HttpResponse r; r.status = status; r.body = body; return r;
    }
    static HttpResponse error(int status, const std::string& message);
    static HttpResponse text(const std::string& body, const std::string& type, int status = 200) {
        HttpResponse r; r.status = status; r.body = body; r.contentType = type; return r;
    }
};

class HttpServer {
public:
    using Handler = std::function<HttpResponse(const HttpRequest&)>;

    HttpServer(std::string bindAddr, int port, size_t maxBodyBytes)
        : bind_(std::move(bindAddr)), port_(port), maxBody_(maxBodyBytes) {}
    ~HttpServer() { stop(); }

    // Pattern segments starting with ':' capture into pathParams, e.g. "/api/datasets/:id/verify".
    void route(const std::string& method, const std::string& pattern, Handler h);
    bool start(std::string& err);
    void stop();

    // Used by tests: parse a raw header block ("GET /x?y=1 HTTP/1.1\r\nHost: a\r\n").
    static bool parseHead(const std::string& head, HttpRequest& req);
    static bool matchRoute(const std::string& pattern, const std::string& path, Params& out);

private:
    struct Route { std::string method, pattern; Handler handler; };
    void acceptLoop();
    void serve(int fd);
    HttpResponse dispatch(HttpRequest& req);

    std::string bind_;
    int port_;
    size_t maxBody_;
    int listenFd_ = -1;
    std::atomic<bool> running_{false};
    std::atomic<int> active_{0};
    std::thread acceptThread_;
    std::vector<Route> routes_;
};

}  // namespace nexus
