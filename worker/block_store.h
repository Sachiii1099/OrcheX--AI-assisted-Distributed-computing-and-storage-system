// BlockStore: the Worker-side half of distributed storage. Block BYTES live here,
// as files under  <root>/blocks/<block_id>.blk  (PostgreSQL only holds metadata).
//
// Safety:
//  * block ids are validated ([A-Za-z0-9_-], <= 64 chars) before they touch the
//    file system, so "../../etc/passwd" can never be addressed;
//  * every store is verified against the expected SHA-256 before it is committed;
//  * files are written to a temp name and renamed, so a crash never leaves a
//    half-written block that looks valid.
#pragma once

#include <string>

namespace nexus {

class BlockStore {
public:
    enum class Status { OK, MISSING, CORRUPT };

    explicit BlockStore(std::string root);

    bool ready() const { return ready_; }
    const std::string& root() const { return root_; }

    // Stores `data` iff sha256(data) == expectedChecksum.
    bool put(const std::string& blockId, const std::string& data,
             const std::string& expectedChecksum, std::string& err);
    bool get(const std::string& blockId, std::string& data, std::string& err) const;
    // Re-reads the file and compares its SHA-256 with `expectedChecksum`.
    Status verify(const std::string& blockId, const std::string& expectedChecksum) const;
    bool remove(const std::string& blockId, std::string& err);
    long long usedBytes() const;

    // Validated path or "" (and err set) for an unsafe id.
    std::string pathFor(const std::string& blockId, std::string& err) const;

private:
    std::string root_;
    std::string blockDir_;
    bool ready_ = false;
};

}  // namespace nexus
