#include "http_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "../common/json.h"
#include "../common/log.h"

namespace nexus {

namespace {
const char* reason(int s) {
    switch (s) {
        case 200: return "OK";
        case 201: return "Created";
        case 202: return "Accepted";
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 409: return "Conflict";
        case 411: return "Length Required";
        case 413: return "Payload Too Large";
        case 500: return "Internal Server Error";
        case 501: return "Not Implemented";
        case 503: return "Service Unavailable";
        default: return "Status";
    }
}

bool sendAll(int fd, const std::string& d) {
    size_t sent = 0;
    while (sent < d.size()) {
        ssize_t n = ::send(fd, d.data() + sent, d.size() - sent, MSG_NOSIGNAL);
        if (n < 0) { if (errno == EINTR) continue; return false; }
        sent += static_cast<size_t>(n);
    }
    return true;
}

// Waits for readable data. false on timeout / error.
bool waitReadable(int fd, int ms) {
    pollfd p{fd, POLLIN, 0};
    return ::poll(&p, 1, ms) > 0;
}
}  // namespace

HttpResponse HttpResponse::error(int status, const std::string& message) {
    HttpResponse r;
    r.status = status;
    r.body = json::Obj().add("error", message).build();
    return r;
}

void HttpServer::route(const std::string& method, const std::string& pattern, Handler h) {
    routes_.push_back({method, pattern, std::move(h)});
}

bool HttpServer::matchRoute(const std::string& pattern, const std::string& path, Params& out) {
    std::vector<std::string> a = splitOn(pattern, '/'), b = splitOn(path, '/');
    if (a.size() != b.size()) return false;
    Params p;
    for (size_t i = 0; i < a.size(); ++i) {
        if (!a[i].empty() && a[i][0] == ':') p[a[i].substr(1)] = urlDecode(b[i]);
        else if (a[i] != b[i]) return false;
    }
    out = p;
    return true;
}

bool HttpServer::parseHead(const std::string& head, HttpRequest& req) {
    size_t eol = head.find("\r\n");
    if (eol == std::string::npos) return false;
    std::string line = head.substr(0, eol);
    size_t s1 = line.find(' '), s2 = line.rfind(' ');
    if (s1 == std::string::npos || s2 == s1) return false;
    req.method = line.substr(0, s1);
    std::string target = line.substr(s1 + 1, s2 - s1 - 1);
    if (target.empty() || target[0] != '/') return false;
    size_t q = target.find('?');
    req.path = urlDecode(target.substr(0, q));
    if (q != std::string::npos) req.query = decodeParams(target.substr(q + 1), true);

    size_t pos = eol + 2;
    while (pos < head.size()) {
        size_t e = head.find("\r\n", pos);
        if (e == std::string::npos) e = head.size();
        std::string h = head.substr(pos, e - pos);
        size_t c = h.find(':');
        if (c != std::string::npos) {
            std::string k = h.substr(0, c);
            for (auto& ch : k) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            req.headers[k] = trim(h.substr(c + 1));
        }
        pos = e + 2;
    }
    return true;
}

bool HttpServer::start(std::string& err) {
    listenFd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd_ < 0) { err = std::strerror(errno); return false; }
    int one = 1;
    ::setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in addr;
    std::memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port_));
    if (inet_pton(AF_INET, bind_.c_str(), &addr.sin_addr) != 1) { err = "invalid bind address '" + bind_ + "'"; ::close(listenFd_); listenFd_ = -1; return false; }
    if (::bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0 || ::listen(listenFd_, 64) != 0) {
        err = std::strerror(errno);
        ::close(listenFd_);
        listenFd_ = -1;
        return false;
    }
    running_ = true;
    acceptThread_ = std::thread(&HttpServer::acceptLoop, this);
    return true;
}

void HttpServer::stop() {
    if (!running_.exchange(false)) return;
    if (acceptThread_.joinable()) acceptThread_.join();
    for (int i = 0; i < 100 && active_ > 0; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(50));
    if (listenFd_ >= 0) { ::close(listenFd_); listenFd_ = -1; }
}

void HttpServer::acceptLoop() {
    while (running_) {
        if (!waitReadable(listenFd_, 300)) continue;
        int fd = ::accept(listenFd_, nullptr, nullptr);
        if (fd < 0) continue;
        if (active_ >= 64) {                      // protect the Master from connection floods
            sendAll(fd, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            ::close(fd);
            continue;
        }
        ++active_;
        std::thread([this, fd] { serve(fd); ::close(fd); --active_; }).detach();
    }
}

HttpResponse HttpServer::dispatch(HttpRequest& req) {
    bool pathMatched = false;
    for (const Route& r : routes_) {
        Params pp;
        if (!matchRoute(r.pattern, req.path, pp)) continue;
        pathMatched = true;
        if (r.method != req.method) continue;
        req.pathParams = pp;
        try { return r.handler(req); }
        catch (const std::exception& e) { return HttpResponse::error(500, std::string("internal error: ") + e.what()); }
    }
    return pathMatched ? HttpResponse::error(405, "method not allowed") : HttpResponse::error(404, "not found: " + req.path);
}

void HttpServer::serve(int fd) {
    // ---- read the header block ----
    std::string buf;
    size_t headEnd = std::string::npos;
    char tmp[8192];
    while (headEnd == std::string::npos) {
        if (buf.size() > 64 * 1024) { sendAll(fd, "HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"); return; }
        if (!waitReadable(fd, 10000)) return;
        ssize_t n = ::recv(fd, tmp, sizeof tmp, 0);
        if (n <= 0) return;
        buf.append(tmp, static_cast<size_t>(n));
        headEnd = buf.find("\r\n\r\n");
    }
    HttpRequest req;
    auto fail = [&](int code, const std::string& msg) {
        HttpResponse r = HttpResponse::error(code, msg);
        sendAll(fd, "HTTP/1.1 " + std::to_string(code) + " " + reason(code) + "\r\nContent-Type: application/json\r\nContent-Length: " +
                        std::to_string(r.body.size()) + "\r\nConnection: close\r\n\r\n" + r.body);
    };
    if (!parseHead(buf.substr(0, headEnd + 2), req)) { fail(400, "malformed request"); return; }

    // ---- body ----
    size_t contentLength = 0;
    auto cl = req.headers.find("content-length");
    if (req.headers.count("transfer-encoding")) { fail(501, "chunked request bodies are not supported; send Content-Length"); return; }
    if (cl != req.headers.end()) {
        try { contentLength = static_cast<size_t>(std::stoull(cl->second)); } catch (...) { fail(400, "bad Content-Length"); return; }
    } else if (req.method == "POST") {
        contentLength = 0;
    }
    if (contentLength > maxBody_) { fail(413, "body larger than the " + std::to_string(maxBody_ / (1024 * 1024)) + " MB limit (NEXUS_MAX_UPLOAD_MB)"); return; }
    std::string body = buf.substr(headEnd + 4);
    if (body.size() < contentLength) {
        auto ex = req.headers.find("expect");
        if (ex != req.headers.end() && upper(ex->second) == "100-CONTINUE") sendAll(fd, "HTTP/1.1 100 Continue\r\n\r\n");
    }
    while (body.size() < contentLength) {
        if (!waitReadable(fd, 15000)) { fail(400, "timeout while reading the request body"); return; }
        ssize_t n = ::recv(fd, tmp, sizeof tmp, 0);
        if (n <= 0) return;
        body.append(tmp, static_cast<size_t>(n));
    }
    body.resize(contentLength);
    req.body = std::move(body);
    auto ct = req.headers.find("content-type");
    // Only small bodies are parsed as forms (a raw file upload sent by curl is labelled form-encoded too).
    if (ct != req.headers.end() && ct->second.find("application/x-www-form-urlencoded") != std::string::npos &&
        req.body.size() <= 1024 * 1024)
        req.form = decodeParams(req.body, true);

    HttpResponse resp = dispatch(req);

    std::string out = "HTTP/1.1 " + std::to_string(resp.status) + " " + reason(resp.status) + "\r\n" +
                      "Content-Type: " + resp.contentType + "\r\n" +
                      "Content-Length: " + std::to_string(resp.body.size()) + "\r\n" +
                      "Cache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n";
    for (const auto& h : resp.headers) out += h.first + ": " + h.second + "\r\n";
    out += "\r\n";
    if (req.method != "HEAD") out += resp.body;
    sendAll(fd, out);
}

}  // namespace nexus
