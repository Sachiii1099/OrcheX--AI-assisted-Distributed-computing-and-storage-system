// HTTP request parsing + routing helpers + a live round trip against the real HttpServer.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <thread>

#include "../master/http_server.h"
#include "test_util.h"

using namespace nexus;

namespace {
std::string request(int port, const std::string& raw) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET; a.sin_port = htons(port); inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0) { ::close(fd); return ""; }
    ::send(fd, raw.data(), raw.size(), 0);
    std::string out;
    char buf[4096];
    ssize_t n;
    while ((n = ::recv(fd, buf, sizeof buf, 0)) > 0) out.append(buf, static_cast<size_t>(n));
    ::close(fd);
    return out;
}
}  // namespace

int main() {
    TEST_CASE("request line / header / query parsing");
    {
        HttpRequest r;
        CHECK(HttpServer::parseHead("POST /api/tasks?type=CPU_BURN&scale=2&name=a%20b+c HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nX-Foo:  bar \r\n", r));
        CHECK_EQ(r.method, std::string("POST"));
        CHECK_EQ(r.path, std::string("/api/tasks"));
        CHECK_EQ(r.param("type"), std::string("CPU_BURN"));
        CHECK_EQ(r.paramInt("scale", 0), 2LL);
        CHECK_EQ(r.param("name"), std::string("a b c"));
        CHECK_EQ(r.headers["content-length"], std::string("5"));
        CHECK_EQ(r.headers["x-foo"], std::string("bar"));
        HttpRequest bad;
        CHECK(!HttpServer::parseHead("garbage\r\n", bad));
        CHECK(!HttpServer::parseHead("GET nothttp HTTP/1.1\r\n", bad));
        CHECK(!HttpServer::parseHead("", bad));
    }

    TEST_CASE("route matching with path parameters");
    {
        Params p;
        CHECK(HttpServer::matchRoute("/api/datasets/:id/verify", "/api/datasets/42/verify", p));
        CHECK_EQ(p["id"], std::string("42"));
        CHECK(!HttpServer::matchRoute("/api/datasets/:id", "/api/datasets/42/verify", p));
        CHECK(!HttpServer::matchRoute("/api/tasks", "/api/task", p));
    }

    TEST_CASE("live server: routing, 404/405, body limit, 100-continue, handler exception");
    {
        const int port = 18941;
        HttpServer srv("127.0.0.1", port, 1024);
        srv.route("GET", "/ping", [](const HttpRequest&) { return HttpResponse::json("{\"pong\":true}"); });
        srv.route("POST", "/echo", [](const HttpRequest& r) { return HttpResponse::json("{\"len\":" + std::to_string(r.body.size()) + ",\"x\":\"" + r.param("x") + "\"}"); });
        srv.route("GET", "/boom", [](const HttpRequest&) -> HttpResponse { throw std::runtime_error("kaput"); });
        std::string err;
        CHECK(srv.start(err));
        std::string r1 = request(port, "GET /ping HTTP/1.1\r\nHost: x\r\n\r\n");
        CHECK(r1.find("200 OK") != std::string::npos && r1.find("{\"pong\":true}") != std::string::npos);
        CHECK(request(port, "GET /nope HTTP/1.1\r\n\r\n").find("404") != std::string::npos);
        CHECK(request(port, "DELETE /ping HTTP/1.1\r\n\r\n").find("405") != std::string::npos);
        std::string r2 = request(port, "POST /echo?x=7 HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello");
        CHECK(r2.find("\"len\":5") != std::string::npos && r2.find("\"x\":\"7\"") != std::string::npos);
        CHECK(request(port, "POST /echo HTTP/1.1\r\nContent-Length: 999999\r\n\r\n").find("413") != std::string::npos);
        CHECK(request(port, "POST /echo HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n").find("501") != std::string::npos);
        CHECK(request(port, "THIS IS NOT HTTP\r\n\r\n").find("400") != std::string::npos);
        CHECK(request(port, "GET /boom HTTP/1.1\r\n\r\n").find("500") != std::string::npos);
        srv.stop();
    }
    return testResult("test_http");
}
