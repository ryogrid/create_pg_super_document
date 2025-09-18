# readCommandResponse

## Location
[src/bin/pgbench/pgbench.c:3241-3382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L3241-L3382)

## Overview
Processes query responses from PostgreSQL backend, handling various result types and implementing error retry logic with optional variable assignment capabilities.

## Definition
```c
static bool readCommandResponse(CState *st, MetaCommand meta, char *varprefix)
```

## Detailed Description
This comprehensive function processes query results returned from PostgreSQL, handling multiple result types including successful commands, SELECT results, pipeline synchronization, and errors. It supports META_GSET and META_ASET operations for storing query results into pgbench variables. The function implements intelligent error handling by categorizing errors and determining retry eligibility through getSQLErrorStatus() and canRetryError(). It processes all results in a loop until no more results are available, properly cleaning up resources on both success and error paths.

## Parameters / Member Variables
- `st`: Pointer to CState structure containing client connection state and execution context
- `meta`: MetaCommand enumeration specifying the type of command (META_NONE, META_GSET, META_ASET, META_ENDPIPELINE)
- `varprefix`: String prefix for variable names when storing results (required for META_GSET/META_ASET, NULL otherwise)

## Dependencies
- Functions called/Symbols referenced:
  - [PQgetResult](../P/PQgetResult.md) (retrieve query results)
  - [PQresultStatus](../P/PQresultStatus.md) (check result status)
  - [PQntuples](../P/PQntuples.md), PQnfields, PQfname, PQgetvalue (result data access)
  - [getSQLErrorStatus](../g/getSQLErrorStatus.md) (categorize SQL errors)
  - [canRetryError](../c/canRetryError.md) (determine retry eligibility)
  - [putVariable](../p/putVariable.md) (store values in pgbench variables)
  - PQexitPipelineMode (exit pipeline mode)
  - [commandError](../c/commandError.md) (error reporting)
  - Various PGRES_* constants and error status enums
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md)

## Notes and Other Information
- Returns true on success, false on any error condition
- Implements proper resource cleanup with PQclear() calls in error handling
- Supports PostgreSQL pipeline mode with PGRES_PIPELINE_SYNC handling
- META_GSET requires exactly one result row, while META_ASET accepts multiple rows
- Stores the last row of results for META_GSET, all rows for META_ASET
- Error retry logic only applies to serialization failures and deadlocks
- The function is static with internal linkage within pgbench.c