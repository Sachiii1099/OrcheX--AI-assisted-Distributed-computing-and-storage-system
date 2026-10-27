// PostgreSQL data-access layer (libpq). One connection guarded by a mutex:
// simple, thread-safe, and plenty for this system. Every query is parameterised
// ($1, $2 ...) so no SQL is ever built from network input.
#pragma once

#include <mutex>
#include <string>
#include <vector>

typedef struct pg_conn PGconn;

namespace nexus {

// Pass as a parameter value to bind SQL NULL.
extern const char* const kSqlNull;

class Database {
public:
    struct Config {
        std::string host, port, name, user, password;
        static Config fromEnv();   // NEXUS_DB_HOST / _PORT / _NAME / _USER / _PASSWORD
    };

    struct Result {
        bool ok = false;
        std::string error;
        int affected = 0;                              // rows changed by INSERT/UPDATE/DELETE
        std::vector<std::string> columns;
        std::vector<unsigned> types;                   // PostgreSQL type OIDs
        std::vector<std::vector<std::string>> rows;
        std::vector<std::vector<char>> nulls;          // 1 where the cell is SQL NULL

        size_t size() const { return rows.size(); }
        bool empty() const { return rows.empty(); }
        const std::string& at(size_t r, size_t c) const { return rows[r][c]; }
        long long i(size_t r, size_t c, long long def = 0) const;
        double d(size_t r, size_t c, double def = 0) const;
        bool isNull(size_t r, size_t c) const { return nulls[r][c] != 0; }
        // Whole result as a JSON array of objects (numbers/booleans/null typed correctly).
        std::string toJson() const;
        // Single row as a JSON object ("null" when there are no rows).
        std::string rowJson(size_t r = 0) const;
    };

    struct Stmt {
        std::string sql;
        std::vector<std::string> params;
    };

    explicit Database(Config cfg) : cfg_(std::move(cfg)) {}
    ~Database();
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    bool connect(std::string& err);
    bool connected();

    // Generic parameterised query.
    Result exec(const std::string& sql, const std::vector<std::string>& params = {});

    // Runs all statements inside one BEGIN/COMMIT; any failure rolls everything back.
    bool transact(const std::vector<Stmt>& stmts, std::string& err);

    // Executes a multi-statement SQL script (schema.sql / seed.sql). No parameters.
    bool runScript(const std::string& sql, std::string& err);
    // True when the NEXUS tables exist (schema applied).
    bool schemaPresent(std::string& missing);

    // ---- Workers -----------------------------------------------------
    bool markAllWorkersInactive();
    bool upsertWorker(const std::string& id, const std::string& hostname,
                      const std::string& ip, int cpuCores, long long memMb, int capacity);
    bool recordHeartbeat(const std::string& id, int running);
    // Updates live state in `workers` AND appends a row to `resource_metrics`.
    struct ResourceRow {
        double cpu = 0, ram = 0, disk = 0, load1 = 0, rssMb = 0;
        long long ramAvailMb = 0, storageBytes = 0;
        int running = 0;
    };
    bool recordResources(const std::string& id, const ResourceRow& r);
    bool setWorkerStatus(const std::string& id, const std::string& status);

private:
    bool ensureConnected(std::string& err);   // caller holds mutex_
    Result execLocked(const std::string& sql, const std::vector<std::string>& params);

    Config cfg_;
    PGconn* conn_ = nullptr;
    std::mutex mutex_;
};

}  // namespace nexus
