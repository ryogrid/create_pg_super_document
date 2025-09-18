# PQexecPrepared

## Location
[src/interfaces/libpq/fe-exec.c:2323-2343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2323-L2343)

## Overview
Executes a previously prepared statement with parameters using PostgreSQL's extended query protocol, providing efficient repeated execution of the same query.

## Definition
```c
PGresult *PQexecPrepared(PGconn *conn,
                         const char *stmtName,
                         int nParams,
                         const char *const *paramValues,
                         const int *paramLengths,
                         const int *paramFormats,
                         int resultFormat)
```

## Detailed Description
PQexecPrepared executes a statement that was previously prepared using PQprepare. This function leverages the server-side prepared statement to avoid re-parsing and re-planning the query, providing better performance for repeatedly executed queries. The function uses the extended query protocol to pass parameters safely and efficiently.

Like other synchronous libpq execution functions, it uses PQexecStart for connection preparation, PQsendQueryPrepared to send the execute message, and PQexecFinish to wait for and retrieve results.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object that must be in a valid state
- `stmtName`: Name of the prepared statement to execute (empty string for unnamed statement)
- `nParams`: Number of parameters to pass (must match the prepared statement's parameter count)
- `paramValues`: Array of parameter value strings, with NULL indicating SQL NULL
- `paramLengths`: Array of parameter lengths for binary format, ignored for text format
- `paramFormats`: Array indicating text (0) or binary (1) format for each parameter
- `resultFormat`: Format for result columns: 0 for text, 1 for binary

## Dependencies
- Functions called/Symbols referenced:
  - [PQexecStart](PQexecStart.md)
  - [PQsendQueryPrepared](PQsendQueryPrepared.md)
  - [PQexecFinish](PQexecFinish.md)
- Called from (representative examples):
  - ecpg_execute
  - [try_complete_step](../t/try_complete_step.md)

## Notes and Other Information
- Returns NULL if the execution request cannot be sent or connection preparation fails
- The prepared statement must exist on the server before calling this function
- Parameter count and types must match those specified when the statement was prepared
- Provides better performance than PQexecParams for repeatedly executed queries
- The returned PGresult must be freed using PQclear() when no longer needed
- Binary parameter formats require careful handling of endianness and type-specific encoding
- Using an empty stmtName executes the unnamed prepared statement