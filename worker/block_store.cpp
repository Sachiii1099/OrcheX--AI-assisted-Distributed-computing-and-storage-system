#include "block_store.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include <climits>
#include <cstdio>
#include <cstdlib>

#include "../common/sha256.h"
#include "../common/util.h"

namespace nexus {

namespace {
bool makeDirs(const std::string& path) {
    std::string cur;
    for (size_t i = 0; i <= path.size(); ++i) {
        if (i == path.size() || path[i] == '/') {
            if (!cur.empty() && ::mkdir(cur.c_str(), 0755) != 0 && errno != EEXIST) return false;
        }
        if (i < path.size()) cur += path[i];
    }
    struct stat st;
    return ::stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}
}  // namespace

BlockStore::BlockStore(std::string root) {
    if (!makeDirs(root)) return;
    char buf[PATH_MAX];
    root_ = ::realpath(root.c_str(), buf) ? std::string(buf) : root;   // canonical root
    blockDir_ = root_ + "/blocks";
    ready_ = makeDirs(blockDir_);
}

std::string BlockStore::pathFor(const std::string& blockId, std::string& err) const {
    if (!isSafeId(blockId)) { err = "invalid block id"; return ""; }
    return blockDir_ + "/" + blockId + ".blk";
}

bool BlockStore::put(const std::string& blockId, const std::string& data,
                     const std::string& expectedChecksum, std::string& err) {
    if (!ready_) { err = "storage directory not available"; return false; }
    std::string path = pathFor(blockId, err);
    if (path.empty()) return false;
    if (sha256Hex(data) != expectedChecksum) { err = "checksum mismatch on receive"; return false; }
    std::string tmp = path + ".tmp";
    if (!writeFile(tmp, data)) { err = "cannot write block file"; ::remove(tmp.c_str()); return false; }
    if (std::rename(tmp.c_str(), path.c_str()) != 0) { err = "cannot commit block file"; ::remove(tmp.c_str()); return false; }
    return true;
}

bool BlockStore::get(const std::string& blockId, std::string& data, std::string& err) const {
    std::string path = pathFor(blockId, err);
    if (path.empty()) return false;
    if (!readFile(path, data)) { err = "block not found on this Worker"; return false; }
    return true;
}

BlockStore::Status BlockStore::verify(const std::string& blockId, const std::string& expectedChecksum) const {
    std::string err, data;
    if (!get(blockId, data, err)) return Status::MISSING;
    return sha256Hex(data) == expectedChecksum ? Status::OK : Status::CORRUPT;
}

bool BlockStore::remove(const std::string& blockId, std::string& err) {
    std::string path = pathFor(blockId, err);
    if (path.empty()) return false;
    if (::remove(path.c_str()) != 0) { err = "block not found"; return false; }
    return true;
}

long long BlockStore::usedBytes() const {
    long long total = 0;
    DIR* d = ::opendir(blockDir_.c_str());
    if (!d) return 0;
    while (dirent* e = ::readdir(d)) {
        std::string name = e->d_name;
        if (name.size() < 5 || name.compare(name.size() - 4, 4, ".blk") != 0) continue;
        struct stat st;
        if (::stat((blockDir_ + "/" + name).c_str(), &st) == 0) total += st.st_size;
    }
    ::closedir(d);
    return total;
}

}  // namespace nexus
