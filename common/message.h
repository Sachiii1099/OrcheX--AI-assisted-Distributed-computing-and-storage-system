// Message framing + TCP connection wrapper shared by Master and Worker.
#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace nexus {

// One application-level message: a type plus string key/value fields.
// Wire format (one line):  TYPE|key=value|key=value\n
// Values are percent-escaped so '|', '=', '%' and newlines are safe.
class Message {
public:
    Message() = default;
    explicit Message(std::string type) : type_(std::move(type)) {}

    Message& set(const std::string& key, const std::string& value);
    Message& setInt(const std::string& key, long long value);
    Message& setDouble(const std::string& key, double value);

    std::string get(const std::string& key, const std::string& def = "") const;
    long long   getInt(const std::string& key, long long def = 0) const;
    double      getDouble(const std::string& key, double def = 0.0) const;

    bool has(const std::string& key) const { return fields_.count(key) != 0; }
    const std::string& type() const { return type_; }

    std::string serialize() const;                       // includes trailing '\n'
    static bool parse(const std::string& line, Message& out);

private:
    std::string type_;
    std::map<std::string, std::string> fields_;
};

// RAII wrapper around a connected TCP socket.
// - send() is thread-safe (heartbeat thread and task threads share a socket).
// - receive() must only be called from ONE reader thread per connection.
class Connection {
public:
    explicit Connection(int fd);
    ~Connection();
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    bool send(const Message& m);
    // Returns 1 = message read, 0 = timeout, -1 = closed / error.
    int receive(Message& out, int timeoutMs);
    void shutdown();                                     // wakes a blocked reader

    std::string peerAddress() const;

    // Client side: socket() + connect(). Returns nullptr and sets err on failure.
    static std::shared_ptr<Connection> connectTo(const std::string& host, int port,
                                                 std::string& err);

private:
    int fd_;
    std::string rbuf_;
    size_t scanPos_ = 0;       // where the next '\n' search starts
    std::mutex sendMutex_;
};

// Server side: socket() + bind() + listen(). Returns listening fd or -1.
int createListener(int port, std::string& err, const std::string& bindAddr = "0.0.0.0");

}  // namespace nexus
