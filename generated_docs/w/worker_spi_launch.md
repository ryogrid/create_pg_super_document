# worker_spi_launch

## Location
[src/test/modules/worker_spi/worker_spi.c:396-493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/worker_spi/worker_spi.c#L396-L493)

## Overview
This function provides a SQL-callable interface for dynamically launching worker_spi background worker processes at runtime with custom database, role, and flag configurations.

## Definition
```c
Datum worker_spi_launch(PG_FUNCTION_ARGS)
```

## Detailed Description
The `worker_spi_launch` function allows users to dynamically create worker_spi background workers through SQL function calls. It differs from the static workers created in _PG_init by:

1. **Dynamic Configuration**: 
   - Accepts runtime parameters for worker ID, database OID, role OID, and flags array
   - Supports custom database and role targeting beyond default GUC values
   - Processes flag array to enable special connection bypass options

2. **Worker Creation Process**:
   - Configures BackgroundWorker structure with dynamic parameters
   - Validates and processes flags array (supports "ALLOWCONN" and "ROLELOGINCHECK" flags)
   - Stores database OID, role OID, and processed flags in bgw_extra for worker_spi_main
   - Sets notification PID to enable startup confirmation

3. **Registration and Startup**:
   - Registers the dynamic worker using RegisterDynamicBackgroundWorker
   - Waits for successful worker startup using WaitForBackgroundWorkerStartup
   - Returns the process ID (PID) of the successfully started worker
   - Provides detailed error reporting for various failure scenarios

This function enables flexible, on-demand worker creation for testing or specialized use cases.

## Parameters / Member Variables
Function arguments accessed via PostgreSQL's function call interface:
- `PG_GETARG_INT32(0)`: Worker index/ID for naming and identification
- `PG_GETARG_OID(1)`: Database OID for worker connection (0 for GUC fallback)
- `PG_GETARG_OID(2)`: Role OID for worker authentication (0 for GUC fallback) 
- `PG_GETARG_ARRAYTYPE_P(3)`: Text array of connection flags ("ALLOWCONN", "ROLELOGINCHECK")

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_* macros, BackgroundWorker/BackgroundWorkerHandle structures
  - BGWORKER_SHMEM_ACCESS, BGWORKER_BACKEND_DATABASE_CONNECTION
  - BgWorkerStart_RecoveryFinished, BGW_NEVER_RESTART, BGW_MAXLEN
  - [RegisterDynamicBackgroundWorker](../R/RegisterDynamicBackgroundWorker.md), WaitForBackgroundWorkerStartup
  - ARR_NDIM, array_contains_nulls, ARR_ELEMTYPE, deconstruct_array_builtin
  - TextDatumGetCString, get_database_oid, get_role_oid
  - BGWORKER_BYPASS_ALLOWCONN, BGWORKER_BYPASS_ROLELOGINCHECK
  - [BgwHandleStatus](../B/BgwHandleStatus.md) constants (BGWH_STOPPED, BGWH_POSTMASTER_DIED, BGWH_STARTED)
- Called from (representative examples):
  - SQL function calls (available as worker_spi_launch SQL function)

## Notes and Other Information
- This function is exposed as a SQL-callable function through PostgreSQL's function call interface
- Supports advanced connection flags that bypass normal connection restrictions
- Implements comprehensive validation of input parameters including array structure checks
- Uses bgw_extra field to pass complex configuration data to worker_spi_main
- Provides synchronous operation - waits for worker startup before returning
- Returns process ID for successful launches, NULL for registration failures
- Includes detailed error handling with appropriate SQL error codes and messages
- Demonstrates dynamic background worker creation patterns for PostgreSQL extensions
- Location: src/test/modules/worker_spi/worker_spi.c:396-493

## Simplified Source
```c
Datum
worker_spi_launch(PG_FUNCTION_ARGS)
{
    int32 worker_id = PG_GETARG_INT32(0);
    Oid dboid = PG_GETARG_OID(1);
    Oid roleoid = PG_GETARG_OID(2);
    ArrayType *flags_array = PG_GETARG_ARRAYTYPE_P(3);

    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;
    BgwHandleStatus status;
    pid_t pid;
    bits32 flags = 0;

    // Configure background worker structure
    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf(worker.bgw_library_name, "worker_spi");
    sprintf(worker.bgw_function_name, "worker_spi_main");
    snprintf(worker.bgw_name, BGW_MAXLEN, "worker_spi dynamic worker %d", worker_id);
    worker.bgw_notify_pid = MyProcPid;  // Enable startup confirmation

    // Process flags array for connection bypass options
    if (ARR_NDIM(flags_array) > 1 || array_contains_nulls(flags_array))
        ereport(ERROR, (errmsg("invalid flags array")));

    // Parse flag strings and set appropriate bypass flags
    Datum *flag_values;
    int nflags;
    deconstruct_array_builtin(flags_array, TEXTOID, &flag_values, NULL, &nflags);

    for (int i = 0; i < nflags; i++) {
        char *flag_name = TextDatumGetCString(flag_values[i]);
        if (strcmp(flag_name, "ALLOWCONN") == 0)
            flags |= BGWORKER_BYPASS_ALLOWCONN;
        else if (strcmp(flag_name, "ROLELOGINCHECK") == 0)
            flags |= BGWORKER_BYPASS_ROLELOGINCHECK;
        else
            ereport(ERROR, (errmsg("unknown flag: %s", flag_name)));
    }

    // Handle database and role defaults
    if (!OidIsValid(dboid))
        dboid = get_database_oid(worker_spi_database, false);
    if (!OidIsValid(roleoid) && worker_spi_role)
        roleoid = get_role_oid(worker_spi_role, false);

    // Pack configuration into bgw_extra
    char *p = worker.bgw_extra;
    memcpy(p, &dboid, sizeof(Oid)); p += sizeof(Oid);
    memcpy(p, &roleoid, sizeof(Oid)); p += sizeof(Oid);
    memcpy(p, &flags, sizeof(bits32));

    // Register and start the dynamic worker
    if (!RegisterDynamicBackgroundWorker(&worker, &handle))
        PG_RETURN_NULL();

    status = WaitForBackgroundWorkerStartup(handle, &pid);

    // Handle startup errors
    if (status != BGWH_STARTED) {
        if (status == BGWH_STOPPED)
            ereport(ERROR, (errmsg("could not start background process")));
        else if (status == BGWH_POSTMASTER_DIED)
            ereport(ERROR, (errmsg("cannot start without postmaster")));
    }

    PG_RETURN_INT32(pid);
}
```