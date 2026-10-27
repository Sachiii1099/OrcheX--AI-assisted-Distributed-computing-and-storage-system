#include "database.h"

#include <libpq-fe.h>

#include <cstdlib>
#include <cstring>

#include "../common/config.h"
#include "../common/json.h"
#include "../common/log.h"

namespace nexus {

const char* const kSqlNull = "\x01NULL";

namespace {

// libpq conninfo values must escape ' and \ and be quoted.
std::string quoteConnValue(const std::string& v) {
    std::string out = "'";
    for (char c : v) {
        if (c == '\'' || c == '\\') out += '\\';
        out += c;
    }
    return out + "'";
}

std::string num(double v) {
    char buf[64];
    std::snprintf(buf, sizeof buf, "%.6f", v);
    return buf;
}

bool isNumericOid(unsigned t) {
    return t == 20 || t == 21 || t == 23 || t == 700 || t == 701 || t == 1700;
}

}  // namespace

// ------------------------------------------------------------------ Result --

long long Database::Result::i(size_t r, size_t c, long long def) const {
    if (r >= rows.size() || c >= rows[r].size() || nulls[r][c]) return def;
    try { return std::stoll(rows[r][c]); } catch (...) { return def; }
}
double Database::Result::d(size_t r, size_t c, double def) const {
    if (r >= rows.size() || c >= rows[r].size() || nulls[r][c]) return def;
    try { return std::stod(rows[r][c]); } catch (...) { return def; }
}

std::string Database::Result::rowJson(size_t r) const {
    if (r >= rows.size()) return "null";
    json::Obj o;
    for (size_t c = 0; c < columns.size(); ++c) {
        if (nulls[r][c]) { o.raw(columns[c], "null"); continue; }
        const std::string& v = rows[r][c];
        if (types[c] == 16) o.raw(columns[c], json::boolean(v == "t"));
        else if (isNumericOid(types[c])) {
            // NaN / Infinity are valid PostgreSQL numerics but not valid JSON.
            char* end = nullptr;
            double dv = std::strtod(v.c_str(), &end);
            if (end && *end == '\0' && std::isfinite(dv)) o.raw(columns[c], v);
            else o.raw(columns[c], "null");
        } else o.raw(columns[c], json::str(v));
    }
    return o.build();
}

std::string Database::Result::toJson() const {
    std::string out = "[";
    for (size_t r = 0; r < rows.size(); ++r) {
        if (r) out += ",";
        out += rowJson(r);
    }
    return out + "]";
}

// ---------------------------------------------------------------- Database --

Database::Config Database::Config::fromEnv() {
    Config c;
    c.host     = envOr("NEXUS_DB_HOST", "localhost");
    c.port     = envOr("NEXUS_DB_PORT", "5432");
    c.name     = envOr("NEXUS_DB_NAME", "nexus");
    c.user     = envOr("NEXUS_DB_USER", "nexus");
    c.password = envOr("NEXUS_DB_PASSWORD", "");   // never hard-coded
    return c;
}

Database::~Database() {
    if (conn_) PQfinish(conn_);
}

bool Database::connect(std::string& err) {
    std::lock_guard<std::mutex> lock(mutex_);
    return ensureConnected(err);
}

bool Database::connected() {
    std::lock_guard<std::mutex> lock(mutex_);
    return conn_ && PQstatus(conn_) == CONNECTION_OK;
}

bool Database::ensureConnected(std::string& err) {
    if (conn_ && PQstatus(conn_) == CONNECTION_OK) return true;

    if (conn_) {
        PQreset(conn_);                       // connection dropped: try to revive
    } else {
        std::string ci = "host=" + quoteConnValue(cfg_.host) +
                         " port=" + quoteConnValue(cfg_.port) +
                         " dbname=" + quoteConnValue(cfg_.name) +
                         " user=" + quoteConnValue(cfg_.user) +
                         " connect_timeout=5";
        if (!cfg_.password.empty()) ci += " password=" + quoteConnValue(cfg_.password);
        conn_ = PQconnectdb(ci.c_str());
    }
    if (!conn_ || PQstatus(conn_) != CONNECTION_OK) {
        err = conn_ ? PQerrorMessage(conn_) : "PQconnectdb returned null";
        while (!err.empty() && (err.back() == '\n' || err.back() == '\r')) err.pop_back();
        return false;
    }
    return true;
}

Database::Result Database::execLocked(const std::string& sql, const std::vector<std::string>& params) {
    Result r;
    if (!ensureConnected(r.error)) return r;

    std::vector<const char*> values;
    for (const auto& p : params) values.push_back(p == kSqlNull ? nullptr : p.c_str());

    PGresult* res = PQexecParams(conn_, sql.c_str(), static_cast<int>(values.size()),
                                 nullptr, values.empty() ? nullptr : values.data(),
                                 nullptr, nullptr, 0);
    ExecStatusType st = PQresultStatus(res);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        r.error = PQerrorMessage(conn_);
        while (!r.error.empty() && (r.error.back() == '\n' || r.error.back() == '\r')) r.error.pop_back();
        log("DB", "Query failed: " + r.error);
        PQclear(res);
        return r;
    }
    const char* aff = PQcmdTuples(res);
    r.affected = (aff && *aff) ? std::atoi(aff) : 0;
    int nf = PQnfields(res), nr = PQntuples(res);
    for (int c = 0; c < nf; ++c) {
        r.columns.push_back(PQfname(res, c));
        r.types.push_back(PQftype(res, c));
    }
    for (int i = 0; i < nr; ++i) {
        std::vector<std::string> row;
        std::vector<char> nl;
        for (int c = 0; c < nf; ++c) {
            bool isnull = PQgetisnull(res, i, c) != 0;
            nl.push_back(isnull ? 1 : 0);
            row.push_back(isnull ? "" : PQgetvalue(res, i, c));
        }
        r.rows.push_back(std::move(row));
        r.nulls.push_back(std::move(nl));
    }
    PQclear(res);
    r.ok = true;
    return r;
}

Database::Result Database::exec(const std::string& sql, const std::vector<std::string>& params) {
    std::lock_guard<std::mutex> lock(mutex_);
    return execLocked(sql, params);
}

bool Database::transact(const std::vector<Stmt>& stmts, std::string& err) {
    std::lock_guard<std::mutex> lock(mutex_);
    Result b = execLocked("BEGIN", {});
    if (!b.ok) { err = b.error; return false; }
    for (const auto& s : stmts) {
        Result r = execLocked(s.sql, s.params);
        if (!r.ok) {
            err = r.error;
            execLocked("ROLLBACK", {});
            return false;
        }
    }
    Result c = execLocked("COMMIT", {});
    if (!c.ok) { err = c.error; return false; }
    return true;
}

bool Database::runScript(const std::string& sql, std::string& err) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!ensureConnected(err)) return false;
    PGresult* res = PQexec(conn_, sql.c_str());   // simple-query protocol: multi-statement OK
    ExecStatusType st = PQresultStatus(res);
    bool ok = st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK;
    if (!ok) err = PQerrorMessage(conn_);
    PQclear(res);
    // drain any remaining results of the multi-statement string
    while ((res = PQgetResult(conn_)) != nullptr) PQclear(res);
    return ok;
}

bool Database::schemaPresent(std::string& missing) {
    static const char* required[] = {
        "workers", "jobs", "tasks", "resource_metrics", "execution_history", "scheduler_config",
        "scheduling_decisions", "datasets", "blocks", "block_replicas", "job_stages",
        "mr_intermediate", "mr_reduce_outputs", "mr_results", "ml_training_runs", "ml_predictions",
        "benchmark_runs", "benchmark_results"};
    for (const char* t : required) {
        Result r = exec("SELECT to_regclass($1) IS NOT NULL", {std::string("public.") + t});
        if (!r.ok || r.empty() || r.at(0, 0) != "t") { missing = t; return false; }
    }
    return true;
}

// ------------------------------------------------------------- Workers ----

bool Database::markAllWorkersInactive() {
    // On Master start no Worker is connected yet; clear stale ACTIVE flags.
    return exec("UPDATE workers SET status='INACTIVE', running_tasks=0").ok;
}

bool Database::upsertWorker(const std::string& id, const std::string& hostname,
                            const std::string& ip, int cpuCores, long long memMb, int capacity) {
    return exec(
        "INSERT INTO workers (worker_id, hostname, ip_address, cpu_cores, memory_total_mb, capacity, "
        "                     status, last_heartbeat) "
        "VALUES ($1, $2, $3, $4::int, $5::bigint, $6::int, 'ACTIVE', now()) "
        "ON CONFLICT (worker_id) DO UPDATE SET "
        "  hostname=EXCLUDED.hostname, ip_address=EXCLUDED.ip_address, "
        "  cpu_cores=EXCLUDED.cpu_cores, memory_total_mb=EXCLUDED.memory_total_mb, "
        "  capacity=EXCLUDED.capacity, status='ACTIVE', last_heartbeat=now()",
        {id, hostname, ip, std::to_string(cpuCores), std::to_string(memMb),
         std::to_string(capacity)}).ok;
}

bool Database::recordHeartbeat(const std::string& id, int running) {
    return exec("UPDATE workers SET status='ACTIVE', last_heartbeat=now(), running_tasks=$2::int "
                "WHERE worker_id=$1", {id, std::to_string(running)}).ok;
}

bool Database::recordResources(const std::string& id, const ResourceRow& r) {
    // One statement: update the live row, then log the sample in resource_metrics.
    return exec(
        "WITH u AS ("
        "  UPDATE workers SET cpu_usage=$2::double precision, ram_usage=$3::double precision, "
        "         disk_usage=$4::double precision, ram_available_mb=$5::bigint, running_tasks=$6::int, "
        "         process_rss_mb=$7::double precision, load1=$8::double precision, "
        "         storage_bytes=$9::bigint, status='ACTIVE', last_heartbeat=now(), last_resource_at=now() "
        "  WHERE worker_id=$1 RETURNING worker_id) "
        "INSERT INTO resource_metrics (worker_id, cpu_usage, ram_usage, disk_usage, status, "
        "                              running_tasks, ram_available_mb) "
        "SELECT worker_id, $2::double precision, $3::double precision, $4::double precision, 'ACTIVE', "
        "       $6::int, $5::bigint FROM u",
        {id, num(r.cpu), num(r.ram), num(r.disk), std::to_string(r.ramAvailMb),
         std::to_string(r.running), num(r.rssMb), num(r.load1), std::to_string(r.storageBytes)}).ok;
}

bool Database::setWorkerStatus(const std::string& id, const std::string& status) {
    return exec("UPDATE workers SET status=$2 WHERE worker_id=$1", {id, status}).ok;
}

}  // namespace nexus
