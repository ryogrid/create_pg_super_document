# pgstat_read_current_status

## Location
[src/backend/utils/activity/backend_status.c:709-884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L709-L884)

## Overview
Creates a local snapshot of the entire PostgreSQL backend status array by copying shared memory status information to process-local memory for consistent access.

## Definition
```c
static void pgstat_read_current_status(void)
```

## Detailed Description
This internal function performs a critical operation for PostgreSQL's activity monitoring system by creating a comprehensive snapshot of all backend processes' status information. The function copies the volatile shared memory backend status array to local memory, ensuring data consistency through careful atomic read protocols.

The function implements a sophisticated copy mechanism that handles concurrent updates to the shared status array. It uses a changecount protocol to detect when status entries are being modified and retries the copy operation until a consistent snapshot is obtained. This is essential because the shared status information is constantly being updated by active backends.

The function allocates memory for storing not just the status structures themselves, but also all the variable-length string data (application names, hostnames, query text) and optional SSL/GSS status information. This local copy remains valid for the duration of the transaction, providing consistent views for functions that need to examine backend status.

## Parameters / Member Variables
This function takes no parameters but operates on several global structures:
- Uses `BackendStatusArray` (shared memory array of backend status entries)
- Populates `localBackendStatusTable` (local copy of status data)
- Updates `localNumBackends` (count of active backends in snapshot)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_setup_backend_status_context](pgstat_setup_backend_status_context.md) (memory context setup)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)/MemoryContextAllocHuge (memory allocation)
  - pgstat_begin_read_activity (atomic read protocol start)
  - pgstat_end_read_activity (atomic read protocol end)
  - pgstat_read_activity_complete (consistency check)
  - [ProcNumberGetTransactionIds](../P/ProcNumberGetTransactionIds.md) (transaction ID retrieval)
  - unvolatize (volatile pointer casting)
- Called from:
  - [pgstat_get_local_beentry_by_proc_number](pgstat_get_local_beentry_by_proc_number.md)
  - [pgstat_get_local_beentry_by_index](pgstat_get_local_beentry_by_index.md)
  - [pgstat_fetch_stat_numbackends](pgstat_fetch_stat_numbackends.md)

## Notes and Other Information
- Function is marked static, indicating internal use within backend_status.c
- Implements a retry mechanism with interruption checking to prevent infinite loops
- Handles optional SSL and GSS status information conditionally compiled
- Uses "huge" allocation for activity strings due to potential size (can exceed 1GB with large configurations)
- The resulting local table is ordered by ProcNumber, which is relied upon by other functions
- Once created, the snapshot persists for the entire transaction to ensure consistency
- Critical for providing stable views of system activity for monitoring and diagnostic queries