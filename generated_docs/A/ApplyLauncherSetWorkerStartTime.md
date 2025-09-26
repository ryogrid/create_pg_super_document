# ApplyLauncherSetWorkerStartTime

## Location
src/backend/replication/logical/launcher.c: 1046 - 1061

## Overview
Records the start time of a logical replication worker for a specific subscription in the shared hash table, enabling tracking of worker launch frequency and preventing rapid restarts.

## Definition
```c
static void ApplyLauncherSetWorkerStartTime(Oid subid, TimestampTz start_time)
```

## Detailed Description
This function stores the timestamp when a logical replication worker was started for a particular subscription. It serves as part of the worker lifecycle management system, allowing the launcher to track when workers were last started to implement policies like restart delays or throttling.

The function first ensures the shared hash table is initialized by calling `logicalrep_launcher_attach_dshmem()`, then uses `dshash_find_or_insert()` to either find an existing entry for the subscription or create a new one. The start time is recorded in the entry, and the lock on the hash table entry is properly released.

This timing information is crucial for preventing excessive worker restarts and implementing backoff strategies when workers fail repeatedly.

## Parameters / Member Variables
- `subid`: The OID of the subscription for which to record the worker start time
- `start_time`: The timestamp when the worker was started (TimestampTz type)

## Dependencies
- Functions called/Symbols referenced:
  - logicalrep_launcher_attach_dshmem
  - dshash_find_or_insert
  - dshash_release_lock
  - LauncherLastStartTimesEntry
- Called from:
  - ApplyLauncherMain

## Notes and Other Information
- This is a static function used internally within the launcher module
- The function automatically handles both new subscriptions (creates entry) and existing ones (updates entry)
- Proper lock management is implemented with `dshash_release_lock()` to prevent deadlocks
- The `found` parameter from `dshash_find_or_insert()` is not used, indicating the function doesn't care whether the entry existed previously
- Part of the worker restart throttling mechanism in PostgreSQL's logical replication system