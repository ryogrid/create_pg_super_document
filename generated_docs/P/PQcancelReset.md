# PQcancelReset

## Location
[src/interfaces/libpq/fe-cancel.c:319-334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-cancel.c#L319-L334)

## Overview
Resets a PostgreSQL cancel connection to its initial state, allowing it to be reused for sending new cancellation requests.

## Definition
```c
void PQcancelReset(PGcancelConn *cancelConn)
```

## Detailed Description
PQcancelReset reinitializes a cancel connection object by closing any existing connection and resetting its internal state to the initial allocated condition. This function enables reuse of PGcancelConn structures for multiple cancellation operations without the need to create new objects. It properly cleans up the underlying connection resources and resets connection tracking variables, preparing the cancel connection for a fresh cancellation attempt.

## Parameters / Member Variables
- `cancelConn`: A pointer to a PGcancelConn structure that will be reset to its initial state

## Dependencies
- Functions called/Symbols referenced:
  - [pqClosePGconn](../p/pqClosePGconn.md)
  - PGcancelConn (type)
  - CONNECTION_ALLOCATED (connection status constant)
- Called from (representative examples):
  - [test_cancel](../t/test_cancel.md) (in libpq_pipeline test module)
  - PQsetdb (referenced in libpq header)

## Notes and Other Information
- The function sets the connection status to CONNECTION_ALLOCATED, indicating the connection object is allocated but not connected
- Resets host and address tracking variables (whichhost, whichaddr) to 0
- Sets host and address retry flags (try_next_host, try_next_addr) to false
- Essential for connection pooling and reuse scenarios in cancellation operations
- Ensures clean state between multiple cancellation attempts on the same connection object
- Does not free the PGcancelConn structure itself, only resets its internal state