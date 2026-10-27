// Tiny helpers for reading configuration from environment variables.
#pragma once

#include <cstdlib>
#include <string>

namespace nexus {

inline std::string envOr(const char* name, const std::string& def) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::string(v) : def;
}

inline int envIntOr(const char* name, int def) {
    const char* v = std::getenv(name);
    if (!v || !*v) return def;
    try { return std::stoi(v); } catch (...) { return def; }
}

}  // namespace nexus
