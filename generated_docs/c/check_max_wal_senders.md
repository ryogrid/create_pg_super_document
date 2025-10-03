# check_max_wal_senders

## Location
[src/backend/utils/init/postinit.c:630-646](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/postinit.c#L630-L646)

## Overview
check_max_wal_senders is a GUC check hook function that validates proposed values for the max_wal_senders configuration parameter to ensure the total backend count doesn't exceed system limits.

## Definition

```c
bool
check_max_wal_senders(int *newval, void **extra, GucSource source)
```
## Detailed Description
This function serves as a validation hook for the max_wal_senders GUC parameter. It verifies that the proposed new value, when combined with MaxConnections, autovacuum workers, background worker processes, and additional system processes, does not exceed the maximum number of backends allowed by the system (MAX_BACKENDS). This validation is crucial for maintaining system stability when configuring WAL streaming replication.

## Parameters / Member Variables
- `*newval`: Pointer to the proposed new value for max_wal_senders
- `**extra`: Pointer to extra data (unused in this function)
- `source`: The source of the configuration change (command line, config file, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - GucSource (type)
  - MAX_BACKENDS
- Global variables used:
  - MaxConnections
  - autovacuum_max_workers
  - max_worker_processes
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- This is a GUC check hook function following PostgreSQL's standard validation pattern
- Returns true if the proposed value is acceptable, false if it would cause the total to exceed MAX_BACKENDS
- The calculation includes +1 to account for additional system overhead
- WAL senders are essential processes for streaming replication, archive recovery, and logical replication
- This validation ensures that configuring too many WAL senders won't compromise system resources
- WAL senders are particularly important for high-availability and disaster recovery setups
- The function is automatically invoked by the GUC system whenever max_wal_senders is being modified
- Each WAL sender process handles streaming WAL data to standby servers or WAL archiving processes