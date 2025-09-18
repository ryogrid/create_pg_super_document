# GetFdwRoutineByServerId

## Location
src/backend/foreign/foreign.c: 377 - 418

## Overview
Retrieves the FdwRoutine structure for a foreign data wrapper by looking up the handler function associated with a given foreign server ID.

## Definition
```c
FdwRoutine *GetFdwRoutineByServerId(Oid serverid)
```

## Detailed Description
This function performs a two-step lookup process to obtain the FdwRoutine structure for a foreign data wrapper. First, it looks up the foreign server in `pg_foreign_server` to get the foreign data wrapper OID. Then it looks up the foreign data wrapper in `pg_foreign_data_wrapper` to get the handler function OID. Finally, it calls the handler function via `GetFdwRoutine` to obtain the FdwRoutine structure containing all the callback functions for the FDW.

The function includes comprehensive error handling for missing servers, missing FDWs, and FDWs configured without handlers (NO HANDLER option).

## Parameters / Member Variables
- `serverid`: The OID of the foreign server for which to retrieve the FDW routine structure

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - elog
  - ObjectIdGetDatum
  - GETSTRUCT
  - ReleaseSysCache
  - OidIsValid
  - ereport
  - errcode
  - errmsg
  - NameStr
  - GetFdwRoutine
  - Form_pg_foreign_data_wrapper
  - Form_pg_foreign_server
- Called from (representative examples):
  - ExecuteTruncateGuts
  - truncate_check_rel
  - ExecInitForeignScan
  - GetFdwRoutineByRelId

## Notes and Other Information
- Performs cascading catalog lookups: server → FDW → handler function
- Returns a pointer to FdwRoutine structure containing FDW callback functions
- Throws ERROR if server lookup fails or if FDW has no handler configured
- Uses system cache for performance optimization
- Essential for initializing foreign scans and other FDW operations