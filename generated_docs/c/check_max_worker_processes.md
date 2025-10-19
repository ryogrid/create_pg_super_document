# check_max_worker_processes

## Location
[src/backend/utils/init/postinit.c:618-629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/postinit.c#L618-L629)

## Overview
check_max_worker_processes is a GUC check hook function that validates proposed values for the max_worker_processes configuration parameter to ensure the total backend count stays within system limits.

## Definition

```c
bool
check_max_worker_processes(int *newval, void **extra, GucSource source)
```
## Detailed Description
This function serves as a validation hook for the max_worker_processes GUC parameter. It checks whether the proposed new value, when combined with MaxConnections, autovacuum workers, WAL senders, and additional system processes, would exceed the maximum number of backends allowed by the system (MAX_BACKENDS). This validation prevents configuration errors that could lead to system resource exhaustion.

## Parameters / Member Variables
- `*newval`: Pointer to the proposed new value for max_worker_processes
- `**extra`: Pointer to extra data (unused in this function)
- `source`: The source of the configuration change (command line, config file, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - GucSource (type)
  - MAX_BACKENDS
- Global variables used:
  - MaxConnections
  - autovacuum_max_workers
  - max_wal_senders
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- This is a GUC check hook function that follows PostgreSQL's standard validation framework
- Returns true if the proposed value is valid, false if it would cause the total to exceed MAX_BACKENDS
- The calculation includes +1 to account for additional system overhead
- Worker processes are used for parallel operations, background tasks, and extensions
- This validation ensures that increasing max_worker_processes won't compromise system stability
- The function is automatically called by the GUC system when max_worker_processes is being set or modified
- Worker processes include parallel query workers, logical replication workers, and custom background workers

## Simplified Source

```c
bool check_max_worker_processes(int *newval, void **extra, GucSource source) {
    // Check if total backend processes would exceed system limit
    // Includes: max_connections + autovacuum workers + new worker processes + WAL senders + 1 overhead
    if (MaxConnections + autovacuum_max_workers + 1 + *newval + max_wal_senders > MAX_BACKENDS)
        return false;

    return true;  // Configuration is valid
}
```