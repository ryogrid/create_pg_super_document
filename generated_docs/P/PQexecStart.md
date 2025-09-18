# PQexecStart

## Location
[src/interfaces/libpq/fe-exec.c:2344-2409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2344-L2409)

## Overview
Internal preparation function that validates connection state and clears any pending results before executing synchronous commands in libpq.

## Definition
```c
static bool PQexecStart(PGconn *conn)
```

## Detailed Description
PQexecStart is a common preparation function used by all synchronous libpq execution functions (PQexec, PQexecParams, PQprepare, PQexecPrepared, etc.). It performs essential validation and cleanup tasks to ensure the connection is ready for a new command execution cycle.

The function handles several critical tasks: validates the connection object, manages error state appropriately for pipeline vs. normal mode, prevents synchronous commands in pipeline mode, and cleans up any residual results from previous operations. It also handles special cases for COPY operations that may be in progress.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object to prepare for command execution

## Dependencies
- Functions called/Symbols referenced:
  - pqClearConnErrorState
  - PQ_PIPELINE_OFF
  - [PQgetResult](PQgetResult.md)
  - ExecStatusType
  - PGRES_COPY_IN
  - [PQputCopyEnd](PQputCopyEnd.md)
  - [libpq_gettext](../l/libpq_gettext.md)
  - PGRES_COPY_OUT
  - PGASYNC_BUSY
  - PGRES_COPY_BOTH
  - CONNECTION_BAD
- Called from (representative examples):
  - [PQexec](PQexec.md)
  - [PQexecParams](PQexecParams.md)
  - [PQprepare](PQprepare.md)
  - [PQexecPrepared](PQexecPrepared.md)
  - [PQdescribePrepared](PQdescribePrepared.md)
  - [PQdescribePortal](PQdescribePortal.md)
  - [PQclosePrepared](PQclosePrepared.md)
  - [PQclosePortal](PQclosePortal.md)

## Notes and Other Information
- Returns false if the connection is invalid or cannot be prepared for execution
- Prevents synchronous command execution when the connection is in pipeline mode
- Automatically discards any unprocessed results from previous operations for backward compatibility
- Handles COPY IN states by sending a termination message to the server
- Handles COPY OUT states by switching connection to busy state and allowing data to be discarded
- Blocks COPY BOTH operations as they are incompatible with synchronous execution
- Clears connection error state only when no commands are queued in pipeline mode
- Essential for maintaining connection state consistency across all synchronous libpq operations