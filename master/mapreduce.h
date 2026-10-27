// MapReduce coordinator (word count) built on the normal task machinery.
//
//   SPLIT    the input is the dataset's blocks (cut at upload time, text-aligned)
//   MAP      one BLOCK_WORD_COUNT task per block, scheduled ONLY on Workers that hold a
//            replica of that block (data locality), output hash-partitioned into R parts
//   SHUFFLE  the Master gathers the R partitions of every map output (mr_intermediate)
//   REDUCE   R REDUCE_WORD_COUNT tasks, one per partition, run on any Worker
//   FINALIZE the merged counts are written to mr_results and the job is COMPLETED
//
// Every stage is a row in job_stages with start / end times. The coordinator is
// stateless: each step is derived from PostgreSQL, so advance() is idempotent and
// the job can resume after a Master restart.
#pragma once

#include <mutex>
#include <string>

#include "../database/database.h"
#include "task_manager.h"

namespace nexus {

class MapReduce {
public:
    MapReduce(Database& db, TaskManager& tasks) : db_(db), tasks_(tasks) {}

    bool start(long long datasetId, int reducers, int priority, long long& jobId, std::string& err);
    void onTaskEvent(const TaskEvent& ev);
    void resume();                               // call once at Master start
    std::string jobJson(long long jobId);        // job, stages with durations, tasks, top words ("" if unknown)

private:
    void advance(long long jobId);
    void failJob(long long jobId, const std::string& why);

    Database& db_;
    TaskManager& tasks_;
    std::mutex mutex_;                           // serialises advance()
};

}  // namespace nexus
