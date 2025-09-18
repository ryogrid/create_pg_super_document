# PQexecFinish

## Location
src/interfaces/libpq/fe-exec.c: 2410 - 2454

## Overview
Internal completion function that waits for and retrieves command results from the server, handling multiple result sets and special cases for synchronous libpq operations.

## Definition
```c
static PGresult *PQexecFinish(PGconn *conn)
```

## Detailed Description
PQexecFinish is the common completion function used by all synchronous libpq execution functions to wait for and retrieve results from the server. It handles the complexity of PostgreSQL's result protocol, which can return multiple result objects for a single command.

The function implements backward compatibility by returning the last result when multiple results are available, automatically concatenating error messages in the connection's error buffer. It also handles special cases for COPY operations and connection failures that require stopping the result retrieval loop.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object from which to retrieve results

## Dependencies
- Functions called/Symbols referenced:
  - PQgetResult
  - PGRES_COPY_IN
  - PGRES_COPY_OUT  
  - PGRES_COPY_BOTH
  - CONNECTION_BAD
- Called from (representative examples):
  - PQexec
  - PQexecParams
  - PQprepare
  - PQexecPrepared
  - PQdescribePrepared
  - PQdescribePortal
  - PQclosePrepared
  - PQclosePortal

## Notes and Other Information
- Returns the last PGresult from a potentially multi-result response sequence
- Automatically frees intermediate results, keeping only the final result
- Stops processing when encountering COPY operations (IN, OUT, or BOTH) to allow application data transfer
- Stops processing if the connection becomes invalid (CONNECTION_BAD)
- Implements backward compatibility behavior for applications expecting single results
- Error message accumulation happens automatically in the connection's errorMessage buffer
- The returned result (if not NULL) must be freed by the caller using PQclear()
- Essential for proper cleanup and result retrieval in synchronous command execution