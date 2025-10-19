# libpqsrv_get_result_last

## Location
[src/include/libpq/libpq-be-fe-helpers.h:290-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/libpq-be-fe-helpers.h#L290-L333)

## Overview
Loops over PQgetResult() calls until completion, returning the last non-NULL result while properly handling interrupts and managing memory.

## Definition
static inline PGresult *libpqsrv_get_result_last(PGconn *conn, uint32 wait_event_info)

## Detailed Description
This function emulates PQexec()'s behavior by continuously calling libpqsrv_get_result() in a loop until no more results are available. It maintains proper memory management by clearing previous results and returning only the last meaningful result. The function handles special terminal states like COPY operations and connection failures, ensuring proper cleanup in case of exceptions through PostgreSQL's PG_TRY/PG_CATCH mechanism.

## Parameters / Member Variables
- conn: PostgreSQL connection handle to retrieve results from
- wait_event_info: Wait event information for monitoring and debugging purposes

## Dependencies
- Functions called/Symbols referenced:
  - PG_TRY
  - [libpqsrv_get_result](libpqsrv_get_result.md)
  - [PQclear](../P/PQclear.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQstatus](../P/PQstatus.md)
  - PGRES_COPY_IN
  - PGRES_COPY_OUT
  - PGRES_COPY_BOTH
  - CONNECTION_BAD
  - PG_CATCH
  - PG_RE_THROW
  - PG_END_TRY
- Called from (representative examples):
  - [libpqsrv_exec](libpqsrv_exec.md)
  - [libpqsrv_exec_params](libpqsrv_exec_params.md)

## Notes and Other Information
- Ensures no PGresult objects are leaked during error conditions through exception handling
- Breaks the result collection loop on COPY operations or connection failures
- Memory management is critical - automatically clears intermediate results to prevent leaks
- Uses PostgreSQL's exception handling mechanism for robust error recovery
- Located in src/include/libpq/libpq-be-fe-helpers.h:290-333

## Simplified Source

```c
static inline PGresult *libpqsrv_get_result_last(PGconn *conn, uint32 wait_event_info) {
    PGresult *volatile lastResult = NULL;

    // Prevent PGresult leaks on exceptions
    PG_TRY();
    {
        for (;;) {
            // Get the next result with interrupt handling
            PGresult *result = libpqsrv_get_result(conn, wait_event_info);
            if (result == NULL)
                break;  // Query complete or failed

            // Keep only the last result, emulating PQexec() behavior
            PQclear(lastResult);
            lastResult = result;

            // Stop on COPY operations or connection failures
            if (PQresultStatus(lastResult) == PGRES_COPY_IN ||
                PQresultStatus(lastResult) == PGRES_COPY_OUT ||
                PQresultStatus(lastResult) == PGRES_COPY_BOTH ||
                PQstatus(conn) == CONNECTION_BAD)
                break;
        }
    }
    PG_CATCH();
    {
        // Clean up on exception
        PQclear(lastResult);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return lastResult;
}
```