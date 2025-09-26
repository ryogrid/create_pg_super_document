# LauncherLastStartTimesEntry

## Location
[src/backend/replication/logical/launcher.c:72-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L72-L76)

## Overview
LauncherLastStartTimesEntry represents an entry in the shared hash table that tracks the last start times of logical replication apply workers for each subscription, enabling restart throttling and preventing rapid worker restart loops.

## Definition
```c
typedef struct LauncherLastStartTimesEntry
{
    Oid         subid;          /* OID of logrep subscription (hash key) */
    TimestampTz last_start_time; /* last time its apply worker was started */
} LauncherLastStartTimesEntry;
```

## Detailed Description
LauncherLastStartTimesEntry is a simple key-value structure used as entries in the dynamic shared hash table maintained by the logical replication launcher. Each entry associates a subscription OID with the timestamp of when its apply worker was last started. This information is crucial for implementing proper restart throttling mechanisms to prevent rapid cycling of failed workers.

The structure is designed to be stored in a dynamic shared hash table (dshash) that can be accessed by multiple processes in the logical replication system. The subscription OID serves as the hash key, allowing for efficient lookups when determining whether enough time has passed since the last worker start to allow a restart.

## Parameters / Member Variables
- `subid`: Object identifier (OID) of the logical replication subscription, used as the hash table key
- `last_start_time`: Timestamp with timezone indicating when the subscription's apply worker was last started

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - TimestampTz (timestamp with timezone type)
- Called from (representative examples):
  - ApplyLauncherSetWorkerStartTime
  - ApplyLauncherGetWorkerStartTime

## Notes and Other Information
- Located in src/backend/replication/logical/launcher.c:71-76
- Used exclusively within the dynamic shared hash table for worker start time tracking
- Essential for preventing worker thrashing by enforcing minimum intervals between restart attempts
- The structure is kept minimal for efficiency in hash table operations
- Part of the logical replication launcher's shared memory management system