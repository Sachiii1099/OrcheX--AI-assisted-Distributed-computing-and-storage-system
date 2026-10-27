# Distributed storage

Code: `common/chunker.h` (splitting + placement), `master/storage_manager.*` (Master side), `worker/block_store.*` (Worker side), tables `datasets`, `blocks`, `block_replicas`.

## 1. Upload workflow

1. **Upload** – dashboard *Storage → Upload* or `POST /api/datasets?name=…&block_size=…&replication=…&text=0|1` with the raw file as the body (≤ `NEXUS_MAX_UPLOAD_MB`, default 64).
2. **Chunking** – `planChunks()` cuts the data into blocks of at most `block_size` bytes (16 B … 4 MiB). With `text=1` each block ends after the last whitespace that fits, so no word is split (needed for MapReduce). Concatenating the blocks in order always reproduces the original.
3. **IDs and checksums** – block id `ds<dataset_id>_b<index>`; each block gets a **SHA-256**; the whole file gets one too (`datasets.sha256`).
4. **Placement** – `chooseReplicaNodes()` takes up to *replication* **distinct** healthy Workers, **emptiest first** (fewest blocks stored, ties → lower id).
5. **Physical write first** – `BLOCK_STORE` carries the bytes (base64) and the checksum; the Worker re-hashes the data, writes `<block>.blk.tmp`, renames it, and replies `ok`.
   *Only after that reply* does the Master record the replica. A failed write (e.g. disk full, Worker lost) makes the Master try the next unused Worker; if no Worker can hold a block the whole upload is aborted and what was written is deleted.
6. **Metadata** – the block row and all its replica rows are inserted in **one transaction** (`Database::transact`). A replica row therefore exists only if the bytes were written and acknowledged.
7. **Fewer Workers than the replication factor** – the block is stored on as many distinct Workers as exist and reported **under-replicated** (never silently counted as fully replicated). With no healthy Worker the upload is refused (409).

## 2. Where the bytes are

```
worker_data/<WORKER_ID>/blocks/<block_id>.blk        (NEXUS_WORKER_STORAGE sets the parent, default ./worker_data)
```
Inspect (in WSL, from the project directory):
```bash
ls -l worker_data/W1/blocks/            # the physical blocks of Worker W1
sha256sum worker_data/W1/blocks/ds1_b0.blk   # equals blocks.checksum for ds1_b0
psql ... -c "select b.block_id, r.worker_id, r.status from blocks b join block_replicas r using(block_id) order by 1,2;"
```
Dashboard: *Storage → click a dataset* shows each block, its replicas, their status and the Worker holding them. PostgreSQL holds only metadata; the bytes exist only on the Workers.

## 3. Reconstruction and checksum validation

* `GET /api/datasets/:id/download` → `reconstruct()`: for each block `fetchBlock()` asks a healthy replica holder (`BLOCK_FETCH`); the Worker returns the data **and the SHA-256 of what is on disk now**; the Master checks it against `blocks.checksum`. A corrupt replica is skipped and the next replica is tried. After all blocks are joined, the **whole-file SHA-256** is compared with `datasets.sha256`.
* If a block has no readable replica the download fails with **409** and lists the unavailable blocks – no partial file is ever returned as if complete.
* `POST …/verify` asks every replica holder to re-hash its copy (`BLOCK_VERIFY`) and sets each replica to `HEALTHY` / `CORRUPT` / `MISSING`; the block state in the API is `OK`, `DEGRADED` (some replicas bad, still readable) or `UNAVAILABLE`.
* Dataset health in the API: `reconstructable`, `blocks_readable`, `healthy_replicas` vs the expected number – **missing replicas are reported accurately**, including replicas on Workers that are currently down.

## 4. Recovery

* `POST …/repair` re-creates `CORRUPT` / `MISSING` replicas from a healthy copy (checksum-checked) on a Worker that does not already hold the block; it reports what it repaired and what it could not.
* Repair is **manual** (button / API), not automatic background re-replication.
* A Worker that restarts finds its blocks still on disk; they are usable again as soon as it re-registers.

## 5. Data locality

`TaskManager` asks `StorageManager::replicaHolders(block)` for the Workers with a HEALTHY replica; for `BLOCK_*` tasks only those Workers are eligible (hard constraint). MapReduce MAP tasks are block tasks, so **computation runs where the data is**. If every holder is down the task waits (`PENDING`) instead of being sent to a Worker that cannot read the block; after a Worker failure the retry is again restricted to replica holders.
Benchmarks report *data-local tasks ÷ block tasks* measured from `block_replicas`.

## 6. Safety

Block ids are validated (`[A-Za-z0-9_-]`, ≤ 64) before touching the file system (no path traversal); Worker directories contain only `<id>.blk` files; every write is verified before commit.

## 7. Tested

`tests/test_storage.cpp` (chunking incl. binary and text-aligned data, reconstruction, checksums, distinct placement), `tests/test_worker_exec.cpp` (block store: checksum, atomicity, traversal) and the `storage` / `mapreduce` sections of `tests/integration_test.py` (29 + 12 checks with real Workers: physical block files exist, byte-exact download, corruption detected, repair, Worker down, restart, replication > Workers).
