# ClearOrSaveAllResults

## Location
[src/bin/psql/common.c:547-560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L547-L560)

## Overview
ClearOrSaveAllResults consumes all remaining query results from the database connection, applying appropriate memory management to each one.

## Definition

```c
static void
ClearOrSaveAllResults(void)
```
## Detailed Description
ClearOrSaveAllResults is a static utility function that exhaustively processes all pending query results from the PostgreSQL connection. It uses a loop to repeatedly call PQgetResult() until no more results are available (indicated by a NULL return), applying ClearOrSaveResult() to each obtained result. This ensures that all results in the connection's result queue are properly handled - error results are preserved for potential \errverbose display while successful results are immediately freed. This function is essential for maintaining connection state consistency and preventing result queue overflow, particularly in scenarios involving multiple result sets or when cleaning up after query execution errors.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [PQgetResult](../P/PQgetResult.md) (PostgreSQL libpq function for retrieving results)
  - [ClearOrSaveResult](ClearOrSaveResult.md) (psql function for result memory management)
- Global variables accessed:
  - pset.db (PostgreSQL database connection handle)
- Called from:
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (src/bin/psql/common.c:1514, 1810)

## Notes and Other Information
This function implements a critical cleanup pattern in PostgreSQL client applications. PQgetResult() may return multiple results for certain operations (such as when multiple statements are sent in a single query string), and it's essential to consume all results to maintain proper protocol synchronization with the server. The function's simple loop-based approach ensures that no results are left pending in the connection queue, which could otherwise interfere with subsequent operations. By delegating the actual result processing to ClearOrSaveResult(), it maintains consistency with psql's error preservation policy while ensuring complete result consumption. This function is typically called in error recovery scenarios or when finalizing query processing to guarantee a clean connection state.