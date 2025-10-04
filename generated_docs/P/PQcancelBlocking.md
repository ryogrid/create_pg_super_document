# PQcancelBlocking

## Location
[src/interfaces/libpq/fe-cancel.c:172-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-cancel.c#L172-L185)

## Overview
Sends a cancellation request in a blocking fashion, providing a simple synchronous interface for query cancellation.

## Definition
```c
int PQcancelBlocking(PGcancelConn *cancelConn)
```

## Detailed Description
PQcancelBlocking provides a blocking/synchronous interface for sending query cancellation requests to a PostgreSQL server. This function acts as a convenience wrapper that combines the non-blocking PQcancelStart() and pqConnectDBComplete() functions to provide a simple, single-call cancellation mechanism.

The function works by:
1. Initiating the cancellation process using PQcancelStart()
2. Blocking until the cancellation request completes using pqConnectDBComplete()

This is the simpler alternative to the non-blocking approach using PQcancelStart() followed by repeated calls to PQcancelPoll(). The function will block the calling thread until the cancellation request either succeeds or fails.

## Parameters / Member Variables
- `cancelConn`: Pointer to a PGcancelConn structure created by PQcancelCreate()

## Dependencies
- Functions called/Symbols referenced:
  - [PQcancelStart](PQcancelStart.md)
  - [pqConnectDBComplete](../p/pqConnectDBComplete.md)
- Called from (representative examples):
  - [disconnectDatabase](../d/disconnectDatabase.md) (src/fe_utils/connect_utils.c:166)
  - [try_complete_step](../t/try_complete_step.md) (src/test/isolation/isolationtester.c:951)
  - [test_cancel](../t/test_cancel.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:288)

## Notes and Other Information
- Returns 1 on successful cancellation, 0 on failure
- This is a blocking operation that may take time to complete depending on network conditions
- For applications requiring non-blocking behavior, use PQcancelStart() and PQcancelPoll() instead
- The cancelConn parameter must be a valid PGcancelConn created by PQcancelCreate()
- Any errors during the cancellation process will be stored in the cancelConn structure and can be retrieved using appropriate error functions

## Simplified Source

```c
int
PQcancelBlocking(PGcancelConn *cancelConn)
{
    // Start the cancellation process
    if (!PQcancelStart(cancelConn)) {
        return 0;
    }

    // Block until cancellation completes
    return pqConnectDBComplete(&cancelConn->conn);
}
```