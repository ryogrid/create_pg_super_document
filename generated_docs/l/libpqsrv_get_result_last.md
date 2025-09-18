# libpqsrv_get_result_last

## Location
src/include/libpq/libpq-be-fe-helpers.h: 290 - 333

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
  - libpqsrv_get_result
  - PQclear
  - PQresultStatus
  - PQstatus
  - PGRES_COPY_IN
  - PGRES_COPY_OUT
  - PGRES_COPY_BOTH
  - CONNECTION_BAD
  - PG_CATCH
  - PG_RE_THROW
  - PG_END_TRY
- Called from (representative examples):
  - libpqsrv_exec
  - libpqsrv_exec_params

## Notes and Other Information
- Ensures no PGresult objects are leaked during error conditions through exception handling
- Breaks the result collection loop on COPY operations or connection failures
- Memory management is critical - automatically clears intermediate results to prevent leaks
- Uses PostgreSQL's exception handling mechanism for robust error recovery
- Located in src/include/libpq/libpq-be-fe-helpers.h:290-333