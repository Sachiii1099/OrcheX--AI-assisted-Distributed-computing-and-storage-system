#!/usr/bin/env bash
# Command-line client for the running Master's HTTP API (curl + python3).
#   ./scripts/nexus_cli.sh status
#   ./scripts/nexus_cli.sh policy [NAME]                        show / set FCFS|ROUND_ROBIN|PRIORITY|RESOURCE_AWARE|AI_ASSISTED
#   ./scripts/nexus_cli.sh submit TYPE [scale] [priority] [block_id]
#   ./scripts/nexus_cli.sh task ID | tasks [N] | workers | history [N] | decisions [N] | concurrency
#   ./scripts/nexus_cli.sh upload FILE [replication] [block_size] [text]   -> dataset id
#   ./scripts/nexus_cli.sh datasets | dataset ID | verify ID | repair ID
#   ./scripts/nexus_cli.sh download ID OUTFILE
#   ./scripts/nexus_cli.sh mapreduce DATASET_ID [reducers] [priority] | job ID
#   ./scripts/nexus_cli.sh train [xgboost|random_forest] | model
#   ./scripts/nexus_cli.sh bench [policies|all] [tasks] [repeats] [type]    start a scheduling benchmark
#   ./scripts/nexus_cli.sh benches | bench-show ID                           list / show benchmark results
. "$(dirname "$0")/common.sh"
master_up || die "Master is not reachable at $API (start it: ./scripts/start_cluster.sh)"
pp() { python3 -m json.tool; }
cmd="${1:-status}"; shift || true
case "$cmd" in
  status)      curl -fsS "$API/api/overview" | pp ;;
  workers)     curl -fsS "$API/api/workers" | pp ;;
  policy)      if [ -n "${1:-}" ]; then curl -sS -X POST "$API/api/policy" -d "policy=$1" | pp; else curl -fsS "$API/api/policy" | pp; fi ;;
  submit)      [ -n "${1:-}" ] || die "usage: submit TYPE [scale] [priority] [block_id]"
               curl -sS -X POST "$API/api/tasks" --data-urlencode "type=$1" -d "scale=${2:-1}&priority=${3:-5}" ${4:+--data-urlencode "block_id=$4"} | pp ;;
  tasks)       curl -fsS "$API/api/tasks?limit=${1:-20}" | pp ;;
  task)        curl -fsS "$API/api/tasks/${1:?task id}" | pp ;;
  history)     curl -fsS "$API/api/history?limit=${1:-20}" | pp ;;
  decisions)   curl -fsS "$API/api/decisions?limit=${1:-20}" | pp ;;
  concurrency) curl -fsS "$API/api/concurrency" | pp ;;
  upload)      f="${1:?file}"; [ -f "$f" ] || die "no such file: $f"
               curl -sS -X POST "$API/api/datasets?name=$(basename "$f")&replication=${2:-2}&block_size=${3:-262144}&text=${4:-0}" \
                    -H 'Content-Type: application/octet-stream' --data-binary "@$f" | pp ;;
  datasets)    curl -fsS "$API/api/datasets" | pp ;;
  dataset)     curl -fsS "$API/api/datasets/${1:?dataset id}" | pp ;;
  verify)      curl -sS -X POST "$API/api/datasets/${1:?dataset id}/verify" | pp ;;
  repair)      curl -sS -X POST "$API/api/datasets/${1:?dataset id}/repair" | pp ;;
  download)    code=$(curl -sS -o "${2:?output file}" -w '%{http_code}' "$API/api/datasets/${1:?dataset id}/download")
               if [ "$code" = "200" ]; then echo "saved $2 ($(wc -c < "$2") bytes, checksum-verified by the Master)"; else cat "$2"; rm -f "$2"; echo; exit 1; fi ;;
  mapreduce)   curl -sS -X POST "$API/api/mapreduce" -d "dataset_id=${1:?dataset id}&reducers=${2:-2}&priority=${3:-5}" | pp ;;
  job)         curl -fsS "$API/api/jobs/${1:?job id}" | pp ;;
  train)       curl -sS -X POST "$API/api/ml/train" -d "model_type=${1:-xgboost}" | pp ;;
  bench)       curl -sS -X POST "$API/api/benchmarks" -d "policies=${1:-all}&count=${2:-24}&repeats=${3:-1}&type=${4:-MIXED}&name=cli-benchmark" | pp ;;
  benches)     curl -fsS "$API/api/benchmarks" | pp ;;
  bench-show)  curl -fsS "$API/api/benchmarks/${1:?benchmark id}" | pp ;;
  model)       curl -fsS "$API/api/ml/status" | pp ;;
  *)           sed -n '2,18p' "$0"; exit 2 ;;
esac
