# worker_spi_launch

## Location
src/test/modules/worker_spi/worker_spi.c: 396 - 493

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
  - RegisterDynamicBackgroundWorker, WaitForBackgroundWorkerStartup
  - ARR_NDIM, array_contains_nulls, ARR_ELEMTYPE, deconstruct_array_builtin
  - TextDatumGetCString, get_database_oid, get_role_oid
  - BGWORKER_BYPASS_ALLOWCONN, BGWORKER_BYPASS_ROLELOGINCHECK
  - BgwHandleStatus constants (BGWH_STOPPED, BGWH_POSTMASTER_DIED, BGWH_STARTED)
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