// Minimal test harness (no third-party framework needed).
#pragma once

#include <cstdio>
#include <string>

namespace nt {
inline int& failures() { static int f = 0; return f; }
inline int& checks() { static int c = 0; return c; }
}  // namespace nt

#define CHECK(cond)                                                                  \
    do {                                                                             \
        ++nt::checks();                                                              \
        if (!(cond)) {                                                               \
            ++nt::failures();                                                        \
            std::printf("  FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond);            \
        }                                                                            \
    } while (0)

#define CHECK_EQ(a, b)                                                               \
    do {                                                                             \
        ++nt::checks();                                                              \
        auto _va = (a);                                                              \
        auto _vb = (b);                                                              \
        if (!(_va == _vb)) {                                                         \
            ++nt::failures();                                                        \
            std::printf("  FAIL %s:%d  %s == %s\n", __FILE__, __LINE__, #a, #b);     \
        }                                                                            \
    } while (0)

#define TEST_CASE(name)  std::printf("[ RUN ] %s\n", name)

inline int testResult(const char* suite) {
    std::printf("%s: %d checks, %d failed\n", suite, nt::checks(), nt::failures());
    return nt::failures() == 0 ? 0 : 1;
}
