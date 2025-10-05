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

## Simplified Source

```c
static void
pgstat_read_current_status(void)
{
    volatile PgBackendStatus *beentry;
    LocalPgBackendStatus *localtable;
    LocalPgBackendStatus *localentry;
    char *localappname, *localclienthostname, *localactivity;
    ProcNumber procNumber;

    // Return if already done in this transaction
    if (localBackendStatusTable)
        return;

    // Setup memory context for backend status snapshot
    pgstat_setup_backend_status_context();

    // Allocate memory for local copies of all status data
    localtable = (LocalPgBackendStatus *)
        MemoryContextAlloc(backendStatusSnapContext,
                          sizeof(LocalPgBackendStatus) * NumBackendStatSlots);
    localappname = (char *)
        MemoryContextAlloc(backendStatusSnapContext,
                          NAMEDATALEN * NumBackendStatSlots);
    localclienthostname = (char *)
        MemoryContextAlloc(backendStatusSnapContext,
                          NAMEDATALEN * NumBackendStatSlots);
    localactivity = (char *)
        MemoryContextAllocHuge(backendStatusSnapContext,
                              pgstat_track_activity_query_size * NumBackendStatSlots);

    localNumBackends = 0;
    beentry = BackendStatusArray;
    localentry = localtable;

    // Copy each backend status entry
    for (procNumber = 0; procNumber < NumBackendStatSlots; procNumber++) {
        // Atomic read protocol with retry on concurrent updates
        for (;;) {
            int before_changecount, after_changecount;

            pgstat_begin_read_activity(beentry, before_changecount);

            // Get process ID first to check if entry is active
            localentry->backendStatus.st_procpid = beentry->st_procpid;

            if (localentry->backendStatus.st_procpid > 0) {
                // Copy the main status structure
                memcpy(&localentry->backendStatus, unvolatize(PgBackendStatus *, beentry),
                       sizeof(PgBackendStatus));

                // Copy string data and update pointers to local copies
                strcpy(localappname, (char *) beentry->st_appname);
                localentry->backendStatus.st_appname = localappname;
                strcpy(localclienthostname, (char *) beentry->st_clienthostname);
                localentry->backendStatus.st_clienthostname = localclienthostname;
                strcpy(localactivity, (char *) beentry->st_activity_raw);
                localentry->backendStatus.st_activity_raw = localactivity;

                // Copy SSL/GSS status if enabled (conditionally compiled)
            }

            pgstat_end_read_activity(beentry, after_changecount);

            // Check if read was consistent
            if (pgstat_read_activity_complete(before_changecount, after_changecount))
                break;

            CHECK_FOR_INTERRUPTS(); // Allow interruption if stuck
        }

        // Include only valid entries in local array
        if (localentry->backendStatus.st_procpid > 0) {
            localentry->proc_number = procNumber;

            // Get transaction IDs for this backend
            ProcNumberGetTransactionIds(procNumber,
                                       &localentry->backend_xid,
                                       &localentry->backend_xmin,
                                       &localentry->backend_subxact_count,
                                       &localentry->backend_subxact_overflowed);

            // Advance pointers to next slot
            localentry++;
            localappname += NAMEDATALEN;
            localclienthostname += NAMEDATALEN;
            localactivity += pgstat_track_activity_query_size;
            localNumBackends++;
        }

        beentry++;
    }

    // Set global pointer only after successful completion
    localBackendStatusTable = localtable;
}
```