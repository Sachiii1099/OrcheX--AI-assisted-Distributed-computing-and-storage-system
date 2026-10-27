// Distributed-storage building blocks that need no network or database, so they
// can be unit-tested: splitting a file into blocks, and choosing which Workers
// hold the replicas of a block.
#pragma once

#include <algorithm>
#include <string>
#include <vector>

#include "sha256.h"

namespace nexus {

struct Chunk {
    size_t index = 0;
    size_t offset = 0;
    size_t size = 0;
    std::string checksum;      // SHA-256 hex of the block bytes
};

// Splits `data` into blocks of at most `blockSize` bytes.
// textAligned=true ends each block just after the last whitespace character
// that fits, so no word is cut in two (needed by MapReduce word count; only a
// single token longer than a whole block can still be split).
// Concatenating the blocks in index order always reproduces `data` exactly.
inline std::vector<Chunk> planChunks(const std::string& data, size_t blockSize, bool textAligned) {
    std::vector<Chunk> out;
    if (blockSize == 0) return out;
    size_t off = 0;
    while (off < data.size()) {
        size_t end = std::min(data.size(), off + blockSize);
        if (textAligned && end < data.size()) {
            size_t p = end;
            while (p > off && !std::isspace(static_cast<unsigned char>(data[p - 1]))) --p;
            if (p > off) end = p;                    // else: one huge token, hard cut
        }
        Chunk c;
        c.index = out.size();
        c.offset = off;
        c.size = end - off;
        c.checksum = sha256Hex(data.data() + off, c.size);
        out.push_back(c);
        off = end;
    }
    return out;
}

struct PlacementNode {
    std::string id;
    long long blocksStored = 0;     // how many blocks the node already holds
};

// Picks up to `replication` DISTINCT nodes, emptiest first (ties: lower id).
// Fewer nodes than `replication` => fewer replicas (caller reports under-replication).
inline std::vector<std::string> chooseReplicaNodes(std::vector<PlacementNode> nodes, int replication) {
    std::sort(nodes.begin(), nodes.end(), [](const PlacementNode& a, const PlacementNode& b) {
        if (a.blocksStored != b.blocksStored) return a.blocksStored < b.blocksStored;
        return a.id < b.id;
    });
    std::vector<std::string> out;
    for (const auto& n : nodes) {
        if (static_cast<int>(out.size()) >= replication) break;
        out.push_back(n.id);
    }
    return out;
}

}  // namespace nexus
