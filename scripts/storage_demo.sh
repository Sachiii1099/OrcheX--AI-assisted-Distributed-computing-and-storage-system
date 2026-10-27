#!/usr/bin/env bash
# Upload a file to the distributed store, show where the replicas live, damage one on purpose,
# detect it, repair it and reconstruct the file.   Needs a running cluster (./scripts/start_cluster.sh).
#   ./scripts/storage_demo.sh [FILE] [REPLICATION] [BLOCK_SIZE]
. "$(dirname "$0")/common.sh"
master_up || die "start the cluster first: ./scripts/start_cluster.sh"
CLI="$ROOT/scripts/nexus_cli.sh"
FILE="${1:-}"; REPL="${2:-2}"; BS="${3:-65536}"
if [ -z "$FILE" ]; then FILE=/tmp/nexus_storage_demo.bin; head -c 600000 /dev/urandom > "$FILE"; echo "(generated 600000 random bytes: $FILE)"; fi
SHA_IN=$(sha256sum "$FILE" | cut -d' ' -f1)

echo "== upload (replication $REPL, block size $BS)"; UP=$("$CLI" upload "$FILE" "$REPL" "$BS" 0); echo "$UP"
DS=$(echo "$UP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["dataset_id"])')
echo "== placement"; curl -fsS "$API/api/datasets/$DS" | python3 -c "
import sys,json; d=json.load(sys.stdin)
for b in d['blocks']: print('  %s %8d B  ->  %s' % (b['block_id'], b['size'], ', '.join(r['worker'] for r in b['replicas'])))"
echo "== damage one replica on disk (flip a byte)"
VICTIM=$(find "$ROOT/worker_data" -name "ds${DS}_b0.blk" | head -1)
[ -n "$VICTIM" ] && { printf '\xff' | dd of="$VICTIM" bs=1 seek=5 count=1 conv=notrunc 2>/dev/null; echo "  damaged $VICTIM"; }
echo "== verify (re-hash every replica on its Worker)"; "$CLI" verify "$DS"
echo "== download still works from the intact replica"; "$CLI" download "$DS" /tmp/nexus_storage_demo.out
[ "$(sha256sum /tmp/nexus_storage_demo.out | cut -d' ' -f1)" = "$SHA_IN" ] && echo "  SHA-256 identical to the original"
echo "== repair"; "$CLI" repair "$DS"
echo "== verify again"; "$CLI" verify "$DS"
