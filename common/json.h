// Minimal JSON *writer* helpers (the API only emits JSON; requests use
// query-string / form encoding, so no JSON parser is needed in C++).
#pragma once

#include <cmath>
#include <cstdio>
#include <string>

namespace nexus {
namespace json {

inline std::string str(const std::string& s) {
    std::string out = "\"";
    for (unsigned char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) { char b[8]; std::snprintf(b, sizeof b, "\\u%04x", c); out += b; }
                else out += static_cast<char>(c);
        }
    }
    return out + "\"";
}

inline std::string num(double v) {
    if (!std::isfinite(v)) return "null";
    char b[64];
    std::snprintf(b, sizeof b, "%.6g", v);
    return b;
}
inline std::string num(long long v) { return std::to_string(v); }
inline std::string num(int v) { return std::to_string(v); }
inline std::string boolean(bool b) { return b ? "true" : "false"; }

// Tiny object builder:  Obj().add("a", 1).addRaw("b", "[1,2]").str()
class Obj {
public:
    Obj& add(const std::string& k, const std::string& v) { return raw(k, str(v)); }
    Obj& add(const std::string& k, const char* v) { return raw(k, str(v)); }
    Obj& add(const std::string& k, double v) { return raw(k, num(v)); }
    Obj& add(const std::string& k, long long v) { return raw(k, num(v)); }
    Obj& add(const std::string& k, int v) { return raw(k, num(v)); }
    Obj& add(const std::string& k, bool v) { return raw(k, boolean(v)); }
    Obj& raw(const std::string& k, const std::string& json) {
        if (!body_.empty()) body_ += ",";
        body_ += str(k) + ":" + json;
        return *this;
    }
    std::string build() const { return "{" + body_ + "}"; }
private:
    std::string body_;
};

}  // namespace json
}  // namespace nexus
