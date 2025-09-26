# ApplyLauncherGetWorkerStartTime

## Location
[src/backend/replication/logical/launcher.c:1062-1087](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L1062-L1087)

## Overview
Retrieves the last recorded start time for a logical replication worker associated with a specific subscription, returning 0 if no previous start time exists.

## Definition
```c
static TimestampTz ApplyLauncherGetWorkerStartTime(Oid subid)
```

## Detailed Description
This function queries the shared hash table to find the last start time recorded for a logical replication worker handling a particular subscription. It's used by the launcher to implement restart throttling policies, allowing the system to enforce minimum intervals between worker restart attempts.

The function first ensures the shared hash table is accessible by calling `logicalrep_launcher_attach_dshmem()`. It then uses `dshash_find()` to search for an existing entry for the given subscription ID. If no entry is found (meaning the worker has never been started or the entry was cleaned up), the function returns 0. If an entry exists, it extracts the timestamp, properly releases the lock, and returns the value.

This timing information is essential for preventing rapid restart loops when workers fail repeatedly, allowing the launcher to implement exponential backoff or other throttling strategies.

## Parameters / Member Variables
- `subid`: The OID of the subscription for which to retrieve the last worker start time

## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_launcher_attach_dshmem](../l/logicalrep_launcher_attach_dshmem.md)
  - [dshash_find](../d/dshash_find.md)
  - [dshash_release_lock](../d/dshash_release_lock.md)
  - [LauncherLastStartTimesEntry](../L/LauncherLastStartTimesEntry.md)
- Called from:
  - [ApplyLauncherMain](ApplyLauncherMain.md)

## Notes and Other Information
- Returns TimestampTz (timestamp with timezone) or 0 if no entry exists
- This is a static function used internally within the launcher module
- The function uses `dshash_find()` with `false` as the third parameter, meaning it will not create an entry if one doesn't exist
- Proper lock management ensures thread safety when accessing the shared hash table
- The return value of 0 for missing entries allows callers to distinguish between 'never started' and actual timestamp values
- Part of PostgreSQL's logical replication worker lifecycle management system