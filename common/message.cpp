#include "message.h"

#include <arpa/inet.h>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0   // main() also ignores SIGPIPE
#endif

namespace nexus {

namespace {

// Block transfers carry base64 block data in one line, so the cap is generous
// (blocks are limited to 4 MiB => ~5.6 MB of base64). Still bounded against a runaway peer.
constexpr size_t kMaxLine = 24 * 1024 * 1024;

bool isHex(char c) { return std::isxdigit(static_cast<unsigned char>(c)) != 0; }

std::string escape(const std::string& s) {
    std::string out;
    for (unsigned char c : s) {
        if (c == '%' || c == '|' || c == '=' || c == '\n' || c == '\r') {
            char buf[4];
            std::snprintf(buf, sizeof buf, "%%%02X", c);
            out += buf;
        } else {
            out += static_cast<char>(c);
        }
    }
    return out;
}

std::string unescape(const std::string& s) {
    std::string out;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size() && isHex(s[i + 1]) && isHex(s[i + 2])) {
            out += static_cast<char>(std::stoi(s.substr(i + 1, 2), nullptr, 16));
            i += 2;
        } else {
            out += s[i];
        }
    }
    return out;
}

}  // namespace

// ---------------------------------------------------------------- Message --

Message& Message::set(const std::string& k, const std::string& v) {
    fields_[k] = v;
    return *this;
}
Message& Message::setInt(const std::string& k, long long v) {
    return set(k, std::to_string(v));
}
Message& Message::setDouble(const std::string& k, double v) {
    char buf[64];
    std::snprintf(buf, sizeof buf, "%.4f", v);
    return set(k, buf);
}

std::string Message::get(const std::string& k, const std::string& def) const {
    auto it = fields_.find(k);
    return it == fields_.end() ? def : it->second;
}
long long Message::getInt(const std::string& k, long long def) const {
    auto it = fields_.find(k);
    if (it == fields_.end()) return def;
    try { return std::stoll(it->second); } catch (...) { return def; }
}
double Message::getDouble(const std::string& k, double def) const {
    auto it = fields_.find(k);
    if (it == fields_.end()) return def;
    try { return std::stod(it->second); } catch (...) { return def; }
}

std::string Message::serialize() const {
    std::string line = type_;
    for (const auto& kv : fields_) line += "|" + kv.first + "=" + escape(kv.second);
    line += "\n";
    return line;
}

bool Message::parse(const std::string& raw, Message& out) {
    std::string line = raw;
    while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) line.pop_back();
    if (line.empty()) return false;

    out = Message();
    size_t pos = line.find('|');
    out.type_ = line.substr(0, pos);
    if (out.type_.empty()) return false;
    while (pos != std::string::npos) {
        size_t next = line.find('|', pos + 1);
        std::string field = line.substr(pos + 1, next == std::string::npos ? next : next - pos - 1);
        size_t eq = field.find('=');
        if (eq != std::string::npos)
            out.fields_[field.substr(0, eq)] = unescape(field.substr(eq + 1));
        pos = next;
    }
    return true;
}

// ------------------------------------------------------------- Connection --

Connection::Connection(int fd) : fd_(fd) {
    int one = 1;   // heartbeats are tiny; don't let Nagle delay them
    ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
}

Connection::~Connection() {
    if (fd_ >= 0) ::close(fd_);
}

void Connection::shutdown() {
    if (fd_ >= 0) ::shutdown(fd_, SHUT_RDWR);
}

bool Connection::send(const Message& m) {
    const std::string data = m.serialize();
    std::lock_guard<std::mutex> lock(sendMutex_);   // keep messages unsplit
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = ::send(fd_, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        sent += static_cast<size_t>(n);
    }
    return true;
}

int Connection::receive(Message& out, int timeoutMs) {
    for (;;) {
        // TCP is a byte stream: one recv() may hold half a message or several.
        // So we buffer and cut complete lines.
        size_t nl = rbuf_.find('\n', scanPos_);
        if (nl != std::string::npos) {
            std::string line = rbuf_.substr(0, nl);
            rbuf_.erase(0, nl + 1);
            scanPos_ = 0;
            if (Message::parse(line, out)) return 1;
            continue;   // skip malformed / empty lines
        }
        scanPos_ = rbuf_.size();          // don't rescan bytes we already searched
        if (rbuf_.size() > kMaxLine) return -1;

        pollfd p;
        p.fd = fd_; p.events = POLLIN; p.revents = 0;
        int r = ::poll(&p, 1, timeoutMs);
        if (r == 0) return 0;
        if (r < 0) return errno == EINTR ? 0 : -1;

        char tmp[65536];
        ssize_t n = ::recv(fd_, tmp, sizeof tmp, 0);
        if (n == 0) return -1;                       // orderly close by peer
        if (n < 0) return (errno == EINTR || errno == EAGAIN) ? 0 : -1;
        rbuf_.append(tmp, static_cast<size_t>(n));
    }
}

std::string Connection::peerAddress() const {
    sockaddr_storage ss;
    socklen_t len = sizeof ss;
    if (::getpeername(fd_, reinterpret_cast<sockaddr*>(&ss), &len) != 0) return "unknown";
    char buf[INET6_ADDRSTRLEN] = {0};
    if (ss.ss_family == AF_INET)
        inet_ntop(AF_INET, &reinterpret_cast<sockaddr_in*>(&ss)->sin_addr, buf, sizeof buf);
    else if (ss.ss_family == AF_INET6)
        inet_ntop(AF_INET6, &reinterpret_cast<sockaddr_in6*>(&ss)->sin6_addr, buf, sizeof buf);
    return buf;
}

std::shared_ptr<Connection> Connection::connectTo(const std::string& host, int port,
                                                  std::string& err) {
    addrinfo hints;
    std::memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    int rc = ::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res);
    if (rc != 0) { err = std::string("getaddrinfo: ") + gai_strerror(rc); return nullptr; }

    int fd = -1;
    for (addrinfo* ai = res; ai; ai = ai->ai_next) {
        fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;
        if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) break;   // connected
        err = std::strerror(errno);
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(res);
    if (fd < 0) return nullptr;
    return std::make_shared<Connection>(fd);
}

int createListener(int port, std::string& err, const std::string& bindAddr) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { err = std::strerror(errno); return -1; }

    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, bindAddr.c_str(), &addr.sin_addr) != 1) {
        err = "invalid bind address '" + bindAddr + "' (use an IPv4 address such as 0.0.0.0 or 127.0.0.1)";
        ::close(fd);
        return -1;
    }
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0 ||
        ::listen(fd, 16) != 0) {
        err = std::strerror(errno);
        ::close(fd);
        return -1;
    }
    return fd;
}

}  // namespace nexus
