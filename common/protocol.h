// NEXUS application-level protocol constants.
//
// TCP only moves bytes. On top of it NEXUS speaks a tiny line-based text
// protocol: one message per line, "TYPE|key=value|key=value\n".
// (See message.h for the encoder/decoder.)
#pragma once

#include <string>
#include <vector>

namespace nexus {
namespace proto {

// ---- Message types -------------------------------------------------------
// Worker -> Master
constexpr const char* REGISTER_WORKER = "REGISTER_WORKER";
constexpr const char* HEARTBEAT       = "HEARTBEAT";
constexpr const char* RESOURCE_UPDATE = "RESOURCE_UPDATE";
constexpr const char* TASK_STARTED    = "TASK_STARTED";
constexpr const char* TASK_RESULT     = "TASK_RESULT";
constexpr const char* TASK_FAILURE    = "TASK_FAILURE";
constexpr const char* BLOCK_REPLY     = "BLOCK_REPLY";     // answer to any BLOCK_* request
// Master -> Worker
constexpr const char* REGISTER_ACK    = "REGISTER_ACK";    // status=OK | REJECTED
constexpr const char* TASK_ASSIGN     = "TASK_ASSIGN";
constexpr const char* BLOCK_STORE     = "BLOCK_STORE";     // req_id, block_id, checksum, data(base64)
constexpr const char* BLOCK_FETCH     = "BLOCK_FETCH";     // req_id, block_id
constexpr const char* BLOCK_VERIFY    = "BLOCK_VERIFY";    // req_id, block_id, checksum
constexpr const char* BLOCK_DELETE    = "BLOCK_DELETE";    // req_id, block_id

// ---- Defaults (overridable with environment variables) -------------------
constexpr int DEFAULT_MASTER_PORT      = 5050;
constexpr int DEFAULT_HTTP_PORT        = 8080;
constexpr int DEFAULT_HEARTBEAT_SEC    = 3;
// A Worker silent for this many heartbeat intervals is considered dead.
constexpr int HEARTBEAT_MISS_FACTOR    = 3;

// ---- Task types ------------------------------------------------------------
// Stand-alone workloads (scale parameter):
//   WORD_COUNT PRIME_COUNT MATRIX_MULTIPLY CHECKSUM CPU_BURN
// Tasks that read a distributed block from the Worker's own storage:
//   BLOCK_WORD_COUNT (MapReduce map)  BLOCK_CHECKSUM
// MapReduce reduce stage (created by the Master, not submitted by users):
//   REDUCE_WORD_COUNT
inline const std::vector<std::string>& allTaskTypes() {
    static const std::vector<std::string> t = {
        "WORD_COUNT", "PRIME_COUNT", "MATRIX_MULTIPLY", "CHECKSUM", "CPU_BURN",
        "BLOCK_WORD_COUNT", "BLOCK_CHECKSUM", "REDUCE_WORD_COUNT"};
    return t;
}
inline bool isKnownTaskType(const std::string& t) {
    for (const auto& k : allTaskTypes()) if (k == t) return true;
    return false;
}
inline bool isBlockTaskType(const std::string& t) {
    return t == "BLOCK_WORD_COUNT" || t == "BLOCK_CHECKSUM";
}
// Types users may submit directly (reduce tasks only exist inside a MapReduce job).
inline bool isUserSubmittable(const std::string& t) {
    return isKnownTaskType(t) && t != "REDUCE_WORD_COUNT";
}

constexpr int MIN_PRIORITY = 0, MAX_PRIORITY = 9, DEFAULT_PRIORITY = 5;
constexpr int DEFAULT_MAX_ATTEMPTS = 3;

}  // namespace proto
}  // namespace nexus
