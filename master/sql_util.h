// SQL snippets shared by the Master modules.
#pragma once

// ISO-8601 UTC timestamp text, e.g. NEXUS_TS("created_at") -> '2026-10-24T12:00:00.123Z'
#define NEXUS_TS(col) "to_char(" col " AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')"
