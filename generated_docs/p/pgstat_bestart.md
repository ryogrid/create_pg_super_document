# pgstat_bestart

## Location
src/backend/utils/activity/backend_status.c: 273 - 439

## Overview
Initializes and populates a backend's entry in the shared PgBackendStatus array with current process information and connection details.

## Definition
```c
void pgstat_bestart(void)
```

## Detailed Description
This function fills in the backend's entry in the shared status array with comprehensive information about the current process, including process ID, backend type, timestamps, database and user IDs, client connection details, and SSL/GSS status when applicable. It uses a careful protocol of copying the shared memory structure to a local variable, modifying it, then copying it back within a critical section to minimize the time spent modifying shared memory and avoid corruption.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - PgBackendStatus, PgBackendSSLStatus, PgBackendGSSStatus
  - memcpy, memset, unvolatize
  - MyProcPid, MyBackendType, MyStartTimestamp, MyDatabaseId
  - GetSessionUserId, MyProcPort
  - B_BACKEND, B_WAL_SENDER, B_BG_WORKER
  - Various SSL functions (be_tls_*)
  - Various GSS functions (be_gssapi_*)
  - PGSTAT_BEGIN_WRITE_ACTIVITY, PGSTAT_END_WRITE_ACTIVITY
  - pgstat_report_appname
  - STATE_UNDEFINED, PROGRESS_COMMAND_INVALID
- Called from:
  - AuxiliaryProcessMainCommon
  - InitPostgres (multiple call sites)

## Notes and Other Information
The function must be called from within a transaction context for non-auxiliary processes since it may need to perform encoding conversion on strings. It handles SSL and GSS status conditionally based on compile-time configuration. The critical section protocol ensures atomic updates to the shared status entry, and the function updates the application name to match the current GUC setting after initialization.