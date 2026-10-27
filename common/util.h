// Small string / parameter / path helpers shared by Master and Worker.
#pragma once

#include <chrono>
#include <cstdint>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace nexus {

using Params = std::map<std::string, std::string>;

inline long long nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

inline std::string trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

inline std::vector<std::string> splitOn(const std::string& s, char sep) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : s) {
        if (c == sep) { out.push_back(cur); cur.clear(); }
        else cur += c;
    }
    out.push_back(cur);
    return out;
}

inline std::string upper(std::string s) {
    for (auto& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return s;
}

// ---- percent-encoding (also used for HTTP query strings / forms) ------------
inline std::string urlEncode(const std::string& s) {
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    for (unsigned char c : s) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') out += static_cast<char>(c);
        else { out += '%'; out += hex[c >> 4]; out += hex[c & 15]; }
    }
    return out;
}

inline std::string urlDecode(const std::string& s, bool plusIsSpace = false) {
    std::string out;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size() + 0 && std::isxdigit(static_cast<unsigned char>(s[i + 1])) &&
            std::isxdigit(static_cast<unsigned char>(s[i + 2]))) {
            out += static_cast<char>(std::stoi(s.substr(i + 1, 2), nullptr, 16));
            i += 2;
        } else if (plusIsSpace && s[i] == '+') {
            out += ' ';
        } else {
            out += s[i];
        }
    }
    return out;
}

// "a=1&b=two%20words"  <->  Params
inline Params decodeParams(const std::string& s, bool plusIsSpace = false) {
    Params p;
    for (const auto& part : splitOn(s, '&')) {
        if (part.empty()) continue;
        size_t eq = part.find('=');
        if (eq == std::string::npos) p[urlDecode(part, plusIsSpace)] = "";
        else p[urlDecode(part.substr(0, eq), plusIsSpace)] = urlDecode(part.substr(eq + 1), plusIsSpace);
    }
    return p;
}

inline std::string encodeParams(const Params& p) {
    std::string out;
    for (const auto& kv : p) {
        if (!out.empty()) out += '&';
        out += urlEncode(kv.first) + "=" + urlEncode(kv.second);
    }
    return out;
}

inline long long paramInt(const Params& p, const std::string& k, long long def) {
    auto it = p.find(k);
    if (it == p.end() || it->second.empty()) return def;
    try { size_t used = 0; long long v = std::stoll(it->second, &used); return used == it->second.size() ? v : def; }
    catch (...) { return def; }
}

inline std::string paramStr(const Params& p, const std::string& k, const std::string& def = "") {
    auto it = p.find(k);
    return it == p.end() ? def : it->second;
}

// ---- identifiers & paths (path-traversal defence) ---------------------------
// Block ids, worker ids and dataset names that become file names must be made of
// [A-Za-z0-9_-] only: no '/', no '.', so "../x" can never be expressed.
inline bool isSafeId(const std::string& id, size_t maxLen = 64) {
    if (id.empty() || id.size() > maxLen) return false;
    for (unsigned char c : id)
        if (!(std::isalnum(c) || c == '_' || c == '-')) return false;
    return true;
}

// Strips directory parts and unsafe characters from a user supplied file name.
inline std::string sanitizeFileName(const std::string& name) {
    size_t slash = name.find_last_of("/\\");
    std::string base = slash == std::string::npos ? name : name.substr(slash + 1);
    std::string out;
    for (unsigned char c : base)
        if (std::isalnum(c) || c == '_' || c == '-' || c == '.') out += static_cast<char>(c);
    while (!out.empty() && out[0] == '.') out.erase(0, 1);   // no hidden / ".." names
    if (out.size() > 100) out.resize(100);
    return out.empty() ? "file" : out;
}

inline bool readFile(const std::string& path, std::string& out) {
    std::ifstream f(path, std::ios::binary);
    if (!f) return false;
    std::ostringstream ss;
    ss << f.rdbuf();
    out = ss.str();
    return true;
}

inline bool writeFile(const std::string& path, const std::string& data) {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f) return false;
    f.write(data.data(), static_cast<std::streamsize>(data.size()));
    return static_cast<bool>(f);
}

inline std::string fmtDouble(double v, int prec = 2) {
    char buf[64];
    std::snprintf(buf, sizeof buf, "%.*f", prec, v);
    return buf;
}

}  // namespace nexus
