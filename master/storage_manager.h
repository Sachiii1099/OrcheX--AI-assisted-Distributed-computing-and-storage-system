// StorageManager: the Master side of the distributed block store.
//
//   upload     split a file into blocks, choose distinct Workers per block, send the
//              bytes (BLOCK_STORE), record dataset / blocks / replicas in PostgreSQL
//   fetchBlock read a block from any healthy replica, validating its SHA-256
//   reconstruct  re-assemble the original file (and check the whole-file SHA-256)
//   verify     ask every replica holder to re-hash its copy; updates replica health
//   repair     re-create missing / corrupt replicas from a healthy copy
//   inspect    JSON view of blocks, replicas and Worker availability
//
// PostgreSQL holds only metadata; the bytes live in the Workers' directories.
#pragma once

#include <map>
#include <set>
#include <string>
#include <vector>

#include "../database/database.h"
#include "worker_manager.h"

namespace nexus {

class StorageManager {
public:
    struct UploadResult {
        bool ok = false;
        std::string error;
        long long datasetId = 0;
        int blocks = 0;
        int underReplicated = 0;      // blocks stored on fewer Workers than requested
        int replicasStored = 0;
        std::string sha256;
    };
    struct ReconstructResult {
        bool ok = false;
        std::string error;
        std::vector<std::string> unavailableBlocks;
    };

    StorageManager(Database& db, WorkerManager& workers) : db_(db), workers_(workers) {}

    UploadResult upload(const std::string& name, const std::string& data, size_t blockSize,
                        int replication, bool textAligned);
    bool fetchBlock(const std::string& blockId, std::string& data, std::string& err);
    ReconstructResult reconstruct(long long datasetId, std::string& out);
    std::string verify(long long datasetId);     // JSON summary
    std::string repair(long long datasetId);     // JSON summary
    std::string inspect(long long datasetId);    // JSON, "" if unknown dataset
    std::string listJson();

    // Workers holding a HEALTHY replica of the block (health of the Worker itself not checked).
    std::set<std::string> replicaHolders(const std::string& blockId);

    static constexpr size_t kMinBlock = 16;                 // bytes (tests use tiny blocks)
    static constexpr size_t kMaxBlock = 4 * 1024 * 1024;

private:
    std::map<std::string, long long> blockCounts();       // blocks stored per Worker (from PostgreSQL)
    bool storeOn(const std::string& workerId, const std::string& blockId,
                 const std::string& checksum, const std::string& data, std::string& err);
    Database& db_;
    WorkerManager& workers_;
};

}  // namespace nexus
