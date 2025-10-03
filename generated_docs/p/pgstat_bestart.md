# pgstat_bestart

## Location
[src/backend/utils/activity/backend_status.c:273-439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L273-L439)

## Overview
Initializes and populates a backend's entry in the shared PgBackendStatus array with current process information and connection details.

## Definition
```c
void pgstat_bestart(void)
```

## Detailed Description
This function fills in the backend's entry in the shared status array with comprehensive information about the current process, including process ID, backend type, timestamps, database and user IDs, client connection details, and SSL/GSS status when applicable. It uses a careful protocol of copying the shared memory structure to a local variable, modifying it, then copying it back within a critical section to minimize the time spent modifying shared memory and avoid corruption.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [PgBackendStatus](../P/PgBackendStatus.md), PgBackendSSLStatus, PgBackendGSSStatus
  - memcpy, memset, unvolatize
  - MyProcPid, MyBackendType, MyStartTimestamp, MyDatabaseId
  - [GetSessionUserId](../G/GetSessionUserId.md), MyProcPort
  - B_BACKEND, B_WAL_SENDER, B_BG_WORKER
  - Various SSL functions (be_tls_*)
  - Various GSS functions (be_gssapi_*)
  - PGSTAT_BEGIN_WRITE_ACTIVITY, PGSTAT_END_WRITE_ACTIVITY
  - [pgstat_report_appname](pgstat_report_appname.md)
  - STATE_UNDEFINED, PROGRESS_COMMAND_INVALID
- Called from:
  - [AuxiliaryProcessMainCommon](../A/AuxiliaryProcessMainCommon.md)
  - [InitPostgres](../I/InitPostgres.md) (multiple call sites)

## Notes and Other Information
The function must be called from within a transaction context for non-auxiliary processes since it may need to perform encoding conversion on strings. It handles SSL and GSS status conditionally based on compile-time configuration. The critical section protocol ensures atomic updates to the shared status entry, and the function updates the application name to match the current GUC setting after initialization.

## Simplified Source

```c
// Simplified version of pgstat_bestart
void pgstat_bestart(void) {
    volatile PgBackendStatus *vbeentry = MyBEEntry;
    PgBackendStatus lbeentry;

    // Step 1: Copy shared memory structure to local variable for safe modification
    memcpy(&lbeentry, unvolatize(PgBackendStatus *, vbeentry), sizeof(PgBackendStatus));

    // Step 2: Fill in basic process information
    lbeentry.st_procpid = MyProcPid;
    lbeentry.st_backendType = MyBackendType;
    lbeentry.st_proc_start_timestamp = MyStartTimestamp;
    lbeentry.st_databaseid = MyDatabaseId;

    // Step 3: Set user ID for relevant backend types
    if (lbeentry.st_backendType == B_BACKEND ||
        lbeentry.st_backendType == B_WAL_SENDER ||
        lbeentry.st_backendType == B_BG_WORKER) {
        lbeentry.st_userid = GetSessionUserId();
    } else {
        lbeentry.st_userid = InvalidOid;
    }

    // Step 4: Set client address information
    if (MyProcPort) {
        memcpy(&lbeentry.st_clientaddr, &MyProcPort->raddr, sizeof(lbeentry.st_clientaddr));
    } else {
        MemSet(&lbeentry.st_clientaddr, 0, sizeof(lbeentry.st_clientaddr));
    }

    // Step 5: Handle SSL status (if enabled)
    #ifdef USE_SSL
    if (MyProcPort && MyProcPort->ssl_in_use) {
        lbeentry.st_ssl = true;
        // Collect SSL connection details
        collect_ssl_info(&lsslstatus, MyProcPort);
    } else {
        lbeentry.st_ssl = false;
    }
    #endif

    // Step 6: Handle GSS status (if enabled)
    #ifdef ENABLE_GSS
    if (MyProcPort && MyProcPort->gss != NULL) {
        lbeentry.st_gss = true;
        // Collect GSS connection details
        collect_gss_info(&lgssstatus, MyProcPort);
    } else {
        lbeentry.st_gss = false;
    }
    #endif

    // Step 7: Initialize state and progress tracking
    lbeentry.st_state = STATE_UNDEFINED;
    lbeentry.st_progress_command = PROGRESS_COMMAND_INVALID;
    lbeentry.st_query_id = 0;

    // Step 8: Atomically update shared memory within critical section
    PGSTAT_BEGIN_WRITE_ACTIVITY(vbeentry);
    lbeentry.st_changecount = vbeentry->st_changecount;
    memcpy(unvolatize(PgBackendStatus *, vbeentry), &lbeentry, sizeof(PgBackendStatus));

    // Initialize string fields
    initialize_string_fields(&lbeentry);

    PGSTAT_END_WRITE_ACTIVITY(vbeentry);

    // Step 9: Update application name from GUC setting
    if (application_name) {
        pgstat_report_appname(application_name);
    }
}
```

Key simplifications made:
- Abstracted SSL and GSS detail collection into conceptual helper functions
- Consolidated timestamp initialization (removed individual timestamp resets)
- Simplified string field initialization into a single conceptual step
- Removed detailed SSL/GSS field-by-field assignments for clarity
- Focused on the main execution flow and critical section protocol
- Preserved the essential copy-to-local, modify, copy-back pattern