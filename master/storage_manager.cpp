#include "storage_manager.h"

#include <algorithm>

#include "../common/chunker.h"
#include "../common/json.h"
#include "../common/log.h"
#include "../common/protocol.h"
#include "../common/sha256.h"
#include "../common/util.h"
#include "sql_util.h"

namespace nexus {

namespace {
const int kRpcTimeoutMs = 30000;

std::string blockIdFor(long long dataset, size_t index) {
    return "ds" + std::to_string(dataset) + "_b" + std::to_string(index);
}
}  // namespace

std::map<std::string, long long> StorageManager::blockCounts() {
    std::map<std::string, long long> m;
    Database::Result r = db_.exec("SELECT worker_id, count(*) FROM block_replicas GROUP BY worker_id");
    for (size_t i = 0; r.ok && i < r.size(); ++i) m[r.at(i, 0)] = r.i(i, 1);
    return m;
}

bool StorageManager::storeOn(const std::string& workerId, const std::string& blockId,
                             const std::string& checksum, const std::string& data, std::string& err) {
    Message req(proto::BLOCK_STORE);
    req.set("block_id", blockId).set("checksum", checksum).set("data", base64Encode(data));
    Message reply;
    if (workers_.call(workerId, req, reply, kRpcTimeoutMs)) return true;
    err = reply.get("error", "unknown error");
    return false;
}

std::set<std::string> StorageManager::replicaHolders(const std::string& blockId) {
    std::set<std::string> s;
    Database::Result r = db_.exec("SELECT worker_id FROM block_replicas WHERE block_id=$1 AND status='HEALTHY'",
                                  {blockId});
    for (size_t i = 0; r.ok && i < r.size(); ++i) s.insert(r.at(i, 0));
    return s;
}

// ---------------------------------------------------------------- upload ----

StorageManager::UploadResult StorageManager::upload(const std::string& rawName, const std::string& data,
                                                    size_t blockSize, int replication, bool textAligned) {
    UploadResult res;
    if (data.empty()) { res.error = "empty file"; return res; }
    if (blockSize < kMinBlock || blockSize > kMaxBlock) {
        res.error = "block size must be between " + std::to_string(kMinBlock) + " and " +
                    std::to_string(kMaxBlock) + " bytes";
        return res;
    }
    if (replication < 1 || replication > 8) { res.error = "replication must be 1..8"; return res; }
    if (workers_.healthyIds().empty()) { res.error = "no healthy Workers available to store blocks"; return res; }

    const std::string name = sanitizeFileName(rawName);
    res.sha256 = sha256Hex(data);
    std::vector<Chunk> chunks = planChunks(data, blockSize, textAligned);

    Database::Result ins = db_.exec(
        "INSERT INTO datasets (name, original_size, block_size, replication, text_aligned, sha256, status) "
        "VALUES ($1, $2::bigint, $3::int, $4::int, $5::boolean, $6, 'UPLOADING') RETURNING dataset_id",
        {name, std::to_string(data.size()), std::to_string(blockSize), std::to_string(replication),
         textAligned ? "true" : "false", res.sha256});
    if (!ins.ok || ins.empty()) { res.error = "database error: " + ins.error; return res; }
    res.datasetId = ins.i(0, 0);

    std::map<std::string, long long> counts = blockCounts();
    std::vector<std::string> storedBlocks;   // for cleanup on failure

    auto abort = [&](const std::string& why) {
        // best-effort removal of what was already written, then drop the catalogue rows
        for (const auto& bid : storedBlocks) {
            Database::Result rr = db_.exec("SELECT worker_id FROM block_replicas WHERE block_id=$1", {bid});
            for (size_t i = 0; rr.ok && i < rr.size(); ++i) {
                Message del(proto::BLOCK_DELETE), reply;
                del.set("block_id", bid);
                workers_.call(rr.at(i, 0), del, reply, 5000);
            }
        }
        db_.exec("DELETE FROM datasets WHERE dataset_id=$1::int", {std::to_string(res.datasetId)});
        res.ok = false;
        res.error = why;
        return res;
    };

    for (const Chunk& c : chunks) {
        const std::string blockId = blockIdFor(res.datasetId, c.index);
        const std::string payload = data.substr(c.offset, c.size);

        std::vector<std::string> stored;
        std::set<std::string> tried;
        while (static_cast<int>(stored.size()) < replication) {
            std::vector<PlacementNode> nodes;
            for (const auto& id : workers_.healthyIds())
                if (!tried.count(id)) nodes.push_back({id, counts[id]});
            if (nodes.empty()) break;
            // distinct Workers, emptiest first
            for (const auto& id : chooseReplicaNodes(nodes, replication - static_cast<int>(stored.size()))) {
                tried.insert(id);
                std::string err;
                if (storeOn(id, blockId, c.checksum, payload, err)) {
                    stored.push_back(id);
                    ++counts[id];
                } else {
                    log("STORAGE", "store of " + blockId + " on " + id + " failed: " + err);
                }
            }
        }
        if (stored.empty()) return abort("could not store block " + blockId + " on any Worker");

        std::vector<Database::Stmt> tx;
        tx.push_back({"INSERT INTO blocks (block_id, dataset_id, block_index, size_bytes, checksum) "
                      "VALUES ($1, $2::int, $3::int, $4::int, $5)",
                      {blockId, std::to_string(res.datasetId), std::to_string(c.index),
                       std::to_string(c.size), c.checksum}});
        for (const auto& w : stored)
            tx.push_back({"INSERT INTO block_replicas (block_id, worker_id, status, last_verified_at) "
                          "VALUES ($1, $2, 'HEALTHY', now())", {blockId, w}});
        std::string err;
        if (!db_.transact(tx, err)) return abort("database error while recording block: " + err);
        storedBlocks.push_back(blockId);
        res.replicasStored += static_cast<int>(stored.size());
        if (static_cast<int>(stored.size()) < replication) ++res.underReplicated;
    }

    Database::Result fin = db_.exec(
        "UPDATE datasets SET block_count=$2::int, status='AVAILABLE' WHERE dataset_id=$1::int",
        {std::to_string(res.datasetId), std::to_string(chunks.size())});
    if (!fin.ok) return abort("database error: " + fin.error);

    res.ok = true;
    res.blocks = static_cast<int>(chunks.size());
    log("STORAGE", "Dataset " + std::to_string(res.datasetId) + " '" + name + "' stored: " +
                   std::to_string(res.blocks) + " blocks, " + std::to_string(res.replicasStored) +
                   " replicas" + (res.underReplicated ? " (" + std::to_string(res.underReplicated) +
                   " blocks under-replicated: fewer Workers than the replication factor)" : ""));
    return res;
}

// ------------------------------------------------------------- retrieval ----

bool StorageManager::fetchBlock(const std::string& blockId, std::string& data, std::string& err) {
    Database::Result b = db_.exec("SELECT checksum FROM blocks WHERE block_id=$1", {blockId});
    if (!b.ok || b.empty()) { err = "unknown block " + blockId; return false; }
    const std::string checksum = b.at(0, 0);

    Database::Result reps = db_.exec(
        "SELECT worker_id FROM block_replicas WHERE block_id=$1 AND status='HEALTHY' ORDER BY worker_id",
        {blockId});
    std::string notes;
    for (size_t i = 0; reps.ok && i < reps.size(); ++i) {
        const std::string w = reps.at(i, 0);
        if (!workers_.isHealthy(w)) { notes += w + ": worker offline; "; continue; }
        Message req(proto::BLOCK_FETCH), reply;
        req.set("block_id", blockId);
        if (!workers_.call(w, req, reply, kRpcTimeoutMs)) { notes += w + ": " + reply.get("error") + "; "; continue; }
        std::string raw;
        if (!base64Decode(reply.get("data"), raw) || sha256Hex(raw) != checksum) {
            notes += w + ": replica failed checksum; ";
            db_.exec("UPDATE block_replicas SET status='CORRUPT', last_verified_at=now() "
                     "WHERE block_id=$1 AND worker_id=$2", {blockId, w});
            log("STORAGE", "Replica of " + blockId + " on " + w + " is CORRUPT - trying the next replica");
            continue;
        }
        data = std::move(raw);
        return true;
    }
    err = "no valid replica of " + blockId + " (" + (notes.empty() ? "no healthy replica recorded" : notes) + ")";
    return false;
}

StorageManager::ReconstructResult StorageManager::reconstruct(long long datasetId, std::string& out) {
    ReconstructResult r;
    Database::Result d = db_.exec("SELECT sha256, original_size, status FROM datasets WHERE dataset_id=$1::int",
                                  {std::to_string(datasetId)});
    if (!d.ok || d.empty()) { r.error = "unknown dataset"; return r; }
    if (d.at(0, 2) != "AVAILABLE") { r.error = "dataset is not AVAILABLE (status " + d.at(0, 2) + ")"; return r; }
    Database::Result bl = db_.exec("SELECT block_id FROM blocks WHERE dataset_id=$1::int ORDER BY block_index",
                                   {std::to_string(datasetId)});
    out.clear();
    out.reserve(static_cast<size_t>(d.i(0, 1)));
    for (size_t i = 0; bl.ok && i < bl.size(); ++i) {
        std::string piece, err;
        if (fetchBlock(bl.at(i, 0), piece, err)) out += piece;
        else r.unavailableBlocks.push_back(bl.at(i, 0));
    }
    if (!r.unavailableBlocks.empty()) {
        out.clear();
        r.error = "dataset cannot be reconstructed: " + std::to_string(r.unavailableBlocks.size()) +
                  " block(s) have no available valid replica";
        return r;
    }
    if (sha256Hex(out) != d.at(0, 0)) {
        out.clear();
        r.error = "reconstructed file does not match the stored whole-file SHA-256";
        return r;
    }
    r.ok = true;
    return r;
}

// ---------------------------------------------------------------- verify ----

std::string StorageManager::verify(long long datasetId) {
    Database::Result d = db_.exec("SELECT replication FROM datasets WHERE dataset_id=$1::int",
                                  {std::to_string(datasetId)});
    if (!d.ok || d.empty()) return "";
    const int wanted = static_cast<int>(d.i(0, 0));
    Database::Result rows = db_.exec(
        "SELECT b.block_id, b.checksum, r.worker_id FROM blocks b "
        "LEFT JOIN block_replicas r ON r.block_id=b.block_id WHERE b.dataset_id=$1::int "
        "ORDER BY b.block_index, r.worker_id", {std::to_string(datasetId)});

    std::map<std::string, int> healthyPerBlock;
    int checked = 0, healthy = 0, corrupt = 0, missing = 0, unreachable = 0;
    for (size_t i = 0; rows.ok && i < rows.size(); ++i) {
        const std::string bid = rows.at(i, 0);
        healthyPerBlock[bid];                                  // make sure every block is listed
        if (rows.isNull(i, 2)) continue;
        const std::string w = rows.at(i, 2);
        if (!workers_.isHealthy(w)) { ++unreachable; continue; }
        Message req(proto::BLOCK_VERIFY), reply;
        req.set("block_id", bid).set("checksum", rows.at(i, 1));
        if (!workers_.call(w, req, reply, kRpcTimeoutMs)) { ++unreachable; continue; }
        ++checked;
        const std::string st = reply.get("state", "MISSING");
        db_.exec("UPDATE block_replicas SET status=$3, last_verified_at=now() WHERE block_id=$1 AND worker_id=$2",
                 {bid, w, st});
        if (st == "HEALTHY") { ++healthy; ++healthyPerBlock[bid]; }
        else if (st == "CORRUPT") ++corrupt;
        else ++missing;
    }
    int degraded = 0;
    std::string lost = "[";
    for (const auto& kv : healthyPerBlock) {
        if (kv.second == 0) { if (lost.size() > 1) lost += ","; lost += json::str(kv.first); }
        else if (kv.second < wanted) ++degraded;
    }
    lost += "]";
    // Replicas that could not be checked (Worker offline) still count as readable
    // if their last known state is HEALTHY - report them separately.
    Database::Result any = db_.exec(
        "SELECT count(*) FROM blocks b WHERE b.dataset_id=$1::int AND NOT EXISTS "
        "(SELECT 1 FROM block_replicas r WHERE r.block_id=b.block_id AND r.status='HEALTHY')",
        {std::to_string(datasetId)});
    const long long noReplica = any.ok && !any.empty() ? any.i(0, 0) : -1;

    json::Obj o;
    o.add("dataset_id", datasetId).add("blocks", static_cast<long long>(healthyPerBlock.size()))
     .add("replicas_checked", checked).add("healthy", healthy).add("corrupt", corrupt)
     .add("missing", missing).add("unreachable", unreachable).add("degraded_blocks", degraded)
     .raw("blocks_without_verified_replica", lost)
     .add("blocks_with_no_healthy_replica_recorded", noReplica)
     .add("reconstructable_now", noReplica == 0 && lost == "[]");
    return o.build();
}

// ---------------------------------------------------------------- repair ----

std::string StorageManager::repair(long long datasetId) {
    Database::Result d = db_.exec("SELECT replication FROM datasets WHERE dataset_id=$1::int",
                                  {std::to_string(datasetId)});
    if (!d.ok || d.empty()) return "";
    const int wanted = static_cast<int>(d.i(0, 0));
    Database::Result bl = db_.exec("SELECT block_id, checksum FROM blocks WHERE dataset_id=$1::int ORDER BY block_index",
                                   {std::to_string(datasetId)});
    std::map<std::string, long long> counts = blockCounts();
    int repaired = 0, stillDegraded = 0;
    std::string unrecoverable = "[";

    for (size_t i = 0; bl.ok && i < bl.size(); ++i) {
        const std::string bid = bl.at(i, 0), checksum = bl.at(i, 1);
        Database::Result reps = db_.exec("SELECT worker_id, status FROM block_replicas WHERE block_id=$1", {bid});
        std::set<std::string> goodOnline, hasRow;
        for (size_t k = 0; reps.ok && k < reps.size(); ++k) {
            hasRow.insert(reps.at(k, 0));
            if (reps.at(k, 1) == "HEALTHY" && workers_.isHealthy(reps.at(k, 0))) goodOnline.insert(reps.at(k, 0));
        }
        const std::vector<std::string> online = workers_.healthyIds();
        int want = std::min<int>(wanted, static_cast<int>(online.size()));
        if (static_cast<int>(goodOnline.size()) >= want) continue;

        std::string data, err;
        if (!fetchBlock(bid, data, err)) {
            if (unrecoverable.size() > 1) unrecoverable += ",";
            unrecoverable += json::str(bid);
            continue;
        }
        while (static_cast<int>(goodOnline.size()) < want) {
            // Prefer re-creating the copy in place on the Worker whose replica is CORRUPT / MISSING
            // (no stale rows left behind); otherwise the emptiest Worker without a healthy copy.
            std::vector<PlacementNode> nodes, inPlace;
            for (const auto& w : online) {
                if (goodOnline.count(w)) continue;
                nodes.push_back({w, counts[w]});
                if (hasRow.count(w)) inPlace.push_back({w, counts[w]});
            }
            if (nodes.empty()) break;
            std::string target = chooseReplicaNodes(inPlace.empty() ? nodes : inPlace, 1).front();
            std::string serr;
            if (!storeOn(target, bid, checksum, data, serr)) {
                log("STORAGE", "repair of " + bid + " on " + target + " failed: " + serr);
                goodOnline.insert(target);          // do not retry this Worker in this pass
                --want;
                continue;
            }
            db_.exec("INSERT INTO block_replicas (block_id, worker_id, status, stored_at, last_verified_at) "
                     "VALUES ($1,$2,'HEALTHY', now(), now()) "
                     "ON CONFLICT (block_id, worker_id) DO UPDATE SET status='HEALTHY', stored_at=now(), last_verified_at=now()",
                     {bid, target});
            ++counts[target];
            goodOnline.insert(target);
            ++repaired;
        }
        if (static_cast<int>(goodOnline.size()) < wanted) ++stillDegraded;
    }
    unrecoverable += "]";
    json::Obj o;
    o.add("dataset_id", datasetId).add("replicas_created", repaired).add("blocks_still_below_replication", stillDegraded)
     .raw("unrecoverable_blocks", unrecoverable);
    if (repaired) log("STORAGE", "Repair of dataset " + std::to_string(datasetId) + ": " + std::to_string(repaired) + " replica(s) re-created");
    return o.build();
}

// ------------------------------------------------------------- inspection ----

std::string StorageManager::inspect(long long datasetId) {
    Database::Result d = db_.exec(
        "SELECT dataset_id, name, original_size, block_size, block_count, replication, text_aligned, sha256, "
        "status, " NEXUS_TS("created_at") " AS created_at FROM datasets WHERE dataset_id=$1::int",
        {std::to_string(datasetId)});
    if (!d.ok || d.empty()) return "";
    Database::Result rows = db_.exec(
        "SELECT b.block_id, b.block_index, b.size_bytes, b.checksum, r.worker_id, r.status, "
        NEXUS_TS("r.last_verified_at") " FROM blocks b LEFT JOIN block_replicas r ON r.block_id=b.block_id "
        "WHERE b.dataset_id=$1::int ORDER BY b.block_index, r.worker_id", {std::to_string(datasetId)});

    const int wanted = static_cast<int>(d.i(0, 5));
    std::string blocks = "[";
    int ok = 0, degraded = 0, unavailable = 0, replicasTotal = 0, replicasGood = 0, nBlocks = 0;
    size_t i = 0;
    while (rows.ok && i < rows.size()) {
        const std::string bid = rows.at(i, 0);
        std::string reps = "[";
        int good = 0, total = 0;
        size_t j = i;
        for (; j < rows.size() && rows.at(j, 0) == bid; ++j) {
            if (rows.isNull(j, 4)) continue;
            const bool online = workers_.isHealthy(rows.at(j, 4));
            const bool usable = online && rows.at(j, 5) == "HEALTHY";
            if (total++) reps += ",";
            json::Obj ro;
            ro.add("worker", rows.at(j, 4)).add("status", rows.at(j, 5)).add("worker_online", online)
              .add("usable", usable);
            if (rows.isNull(j, 6)) ro.raw("last_verified_at", "null"); else ro.add("last_verified_at", rows.at(j, 6));
            reps += ro.build();
            if (usable) ++good;
        }
        reps += "]";
        const char* state = good == 0 ? "UNAVAILABLE" : (good < wanted ? "DEGRADED" : "OK");
        if (good == 0) ++unavailable; else if (good < wanted) ++degraded; else ++ok;
        replicasTotal += total;
        replicasGood += good;
        json::Obj bo;
        bo.add("block_id", bid).add("index", static_cast<long long>(rows.i(i, 1))).add("size", static_cast<long long>(rows.i(i, 2)))
          .add("checksum", rows.at(i, 3)).add("usable_replicas", good).add("state", state).raw("replicas", reps);
        if (nBlocks++) blocks += ",";
        blocks += bo.build();
        i = j;
    }
    blocks += "]";

    std::string head = d.rowJson(0);
    head.pop_back();                                   // reopen the object and append computed fields
    json::Obj extra;
    extra.add("blocks_ok", ok).add("blocks_degraded", degraded).add("blocks_unavailable", unavailable)
         .add("replicas_total", replicasTotal).add("replicas_usable", replicasGood)
         .add("reconstructable", unavailable == 0 && nBlocks > 0)
         .add("health", unavailable ? "UNAVAILABLE" : (degraded ? "DEGRADED" : "OK"));
    std::string ex = extra.build();                    // "{...}" -> strip braces
    return head + "," + ex.substr(1, ex.size() - 2) + ",\"blocks\":" + blocks + "}";
}

std::string StorageManager::listJson() {
    Database::Result d = db_.exec(
        "SELECT d.dataset_id, d.name, d.original_size, d.block_size, d.block_count, d.replication, "
        "d.text_aligned, d.sha256, d.status, " NEXUS_TS("d.created_at") " AS created_at, "
        "(SELECT count(*) FROM block_replicas r JOIN blocks b ON b.block_id=r.block_id WHERE b.dataset_id=d.dataset_id) AS replicas, "
        "(SELECT count(*) FROM block_replicas r JOIN blocks b ON b.block_id=r.block_id WHERE b.dataset_id=d.dataset_id AND r.status='HEALTHY') AS healthy_replicas "
        "FROM datasets d ORDER BY d.dataset_id DESC LIMIT 200");
    if (!d.ok) return "[]";
    // availability: a block is readable if one HEALTHY replica sits on an online Worker
    Database::Result rep = db_.exec(
        "SELECT b.dataset_id, b.block_id, r.worker_id FROM blocks b "
        "LEFT JOIN block_replicas r ON r.block_id=b.block_id AND r.status='HEALTHY'");
    std::map<long long, std::map<std::string, bool>> avail;      // dataset -> block -> readable
    for (size_t i = 0; rep.ok && i < rep.size(); ++i) {
        bool& a = avail[rep.i(i, 0)][rep.at(i, 1)];
        if (!rep.isNull(i, 2) && workers_.isHealthy(rep.at(i, 2))) a = true;
    }
    std::string out = "[";
    for (size_t i = 0; i < d.size(); ++i) {
        int readable = 0, total = 0;
        for (const auto& kv : avail[d.i(i, 0)]) { ++total; if (kv.second) ++readable; }
        std::string row = d.rowJson(i);
        row.pop_back();
        row += ",\"blocks_readable\":" + std::to_string(readable) + ",\"reconstructable\":" +
               json::boolean(total > 0 && readable == total) + "}";
        if (i) out += ",";
        out += row;
    }
    return out + "]";
}

}  // namespace nexus
