// Distributed storage logic that needs no network: chunking, checksums, reconstruction,
// replica placement, text alignment.
#include <random>

#include "../common/chunker.h"
#include "../common/sha256.h"
#include "test_util.h"

using namespace nexus;

namespace {
std::string reassemble(const std::string& data, const std::vector<Chunk>& cs) {
    std::string out;
    for (const auto& c : cs) out += data.substr(c.offset, c.size);
    return out;
}
}  // namespace

int main() {
    TEST_CASE("chunking: exact reconstruction, sizes, per-block checksums");
    {
        std::mt19937 rng(1);
        std::string data(100000, 0);
        for (auto& ch : data) ch = static_cast<char>(rng() & 0xFF);       // arbitrary binary
        for (size_t bs : {16u, 1000u, 4096u, 65536u, 100000u, 200000u}) {
            auto cs = planChunks(data, bs, false);
            CHECK(reassemble(data, cs) == data);
            CHECK_EQ(cs.size(), (data.size() + bs - 1) / bs);
            size_t sum = 0;
            for (size_t i = 0; i < cs.size(); ++i) {
                CHECK_EQ(cs[i].index, i);
                CHECK(cs[i].size <= bs && cs[i].size > 0);
                CHECK_EQ(cs[i].checksum, sha256Hex(data.substr(cs[i].offset, cs[i].size)));
                sum += cs[i].size;
            }
            CHECK_EQ(sum, data.size());
        }
        CHECK(planChunks("", 100, false).empty());
        CHECK(planChunks("abc", 0, false).empty());
    }

    TEST_CASE("text-aligned chunking never splits a word");
    {
        std::string text;
        std::mt19937 rng(2);
        for (int i = 0; i < 3000; ++i) { text += std::string(1 + rng() % 9, static_cast<char>('a' + rng() % 26)); text += (rng() % 7 ? " " : "\n"); }
        for (size_t bs : {64u, 500u, 4096u}) {
            auto cs = planChunks(text, bs, true);
            CHECK(reassemble(text, cs) == text);
            for (size_t i = 0; i + 1 < cs.size(); ++i) {
                CHECK(cs[i].size <= bs);
                char last = text[cs[i].offset + cs[i].size - 1];
                CHECK(std::isspace(static_cast<unsigned char>(last)));        // every block but the last ends at a word break
            }
        }
        // a single token longer than the block cannot be aligned: hard cut, still lossless
        std::string longTok(500, 'x');
        auto cs = planChunks(longTok, 100, true);
        CHECK_EQ(cs.size(), 5u);
        CHECK(reassemble(longTok, cs) == longTok);
    }

    TEST_CASE("replica placement: distinct workers, balanced, degrades gracefully");
    {
        std::vector<PlacementNode> nodes = {{"W1", 5}, {"W2", 0}, {"W3", 2}};
        auto two = chooseReplicaNodes(nodes, 2);
        CHECK_EQ(two.size(), 2u);
        CHECK(two[0] == "W2" && two[1] == "W3");                              // emptiest first
        CHECK(two[0] != two[1]);
        auto three = chooseReplicaNodes(nodes, 3);
        CHECK_EQ(three.size(), 3u);
        auto five = chooseReplicaNodes(nodes, 5);                             // fewer workers than replicas
        CHECK_EQ(five.size(), 3u);
        CHECK(chooseReplicaNodes({}, 2).empty());
        // simulate placing 30 blocks x 2 replicas on 3 workers: never the same worker twice, even spread
        std::vector<PlacementNode> cl = {{"A", 0}, {"B", 0}, {"C", 0}};
        for (int b = 0; b < 30; ++b) {
            auto pick = chooseReplicaNodes(cl, 2);
            CHECK(pick[0] != pick[1]);
            for (auto& id : pick) for (auto& n : cl) if (n.id == id) ++n.blocksStored;
        }
        CHECK_EQ(cl[0].blocksStored, 20); CHECK_EQ(cl[1].blocksStored, 20); CHECK_EQ(cl[2].blocksStored, 20);
        // ties are deterministic
        std::vector<PlacementNode> tie = {{"B", 1}, {"A", 1}};
        CHECK_EQ(chooseReplicaNodes(tie, 1)[0], std::string("A"));
    }

    // Replica failure / unavailable-replica behaviour (corruption, dead Workers, 409 "cannot reconstruct")
    // needs real Workers and is covered by tests/integration_test.py (section "storage").
    return testResult("test_storage");
}
