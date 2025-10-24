# disconnectDatabase

## Location
[src/fe_utils/connect_utils.c:158-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/connect_utils.c#L158-L171)

## Overview
Safely disconnects a PostgreSQL database connection, ensuring any active transactions are properly canceled before termination.

## Definition

```c
void
disconnectDatabase(PGconn *conn)
```
## Detailed Description
The `disconnectDatabase` function provides a clean and safe way to terminate PostgreSQL database connections. It implements proper connection cleanup by first checking for active transactions and canceling them if necessary before closing the connection. This prevents potential issues that could arise from abruptly terminating connections with ongoing database operations.

The disconnection process follows these steps:
1. Assert that the connection pointer is not NULL
2. Check if there's an active transaction using PQtransactionStatus()
3. If a transaction is active (PQTRANS_ACTIVE), create a cancel request
4. Execute the cancel request using blocking cancellation
5. Clean up the cancel connection object
6. Finally, close the database connection with PQfinish()

This approach ensures that database resources are properly released and that no hanging transactions remain on the server side.

## Parameters / Member Variables
- `conn`: PostgreSQL connection handle to be disconnected (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [PQtransactionStatus](../P/PQtransactionStatus.md) (check transaction state)
  - [PQcancelCreate](../P/PQcancelCreate.md) (create cancellation handle)
  - [PQcancelBlocking](../P/PQcancelBlocking.md) (execute cancellation)
  - [PQcancelFinish](../P/PQcancelFinish.md) (cleanup cancellation handle)
  - [PQfinish](../P/PQfinish.md) (close connection)
  - Assert (debug assertion)
  - PQTRANS_ACTIVE (transaction status constant)
  - [PGcancelConn](../P/PGcancelConn.md) (cancellation connection type)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_amcheck - multiple locations)
  - [compile_database_list](../c/compile_database_list.md)
  - [compile_relation_list_one_db](../c/compile_relation_list_one_db.md)
  - [ParallelSlotsGetIdle](../P/ParallelSlotsGetIdle.md)
  - [ParallelSlotsAdoptConn](../P/ParallelSlotsAdoptConn.md)
  - [ParallelSlotsTerminate](../P/ParallelSlotsTerminate.md)

## Notes and Other Information
- Essential for preventing connection leaks in PostgreSQL client applications
- The cancellation mechanism ensures that long-running queries are properly terminated
- Used extensively in parallel processing scenarios where multiple connections need cleanup
- Part of the frontend utilities library for consistent connection management
- The function uses blocking cancellation, which waits for the cancel request to complete
- Located in src/fe_utils/connect_utils.c:158-171
- Critical for proper resource management in PostgreSQL client tools

## Simplified Source

```c
void disconnectDatabase(PGconn *conn) {
    Assert(conn != NULL);

    // Cancel any active transaction before closing
    if (PQtransactionStatus(conn) == PQTRANS_ACTIVE) {
        PGcancelConn *cancelConn = PQcancelCreate(conn);
        PQcancelBlocking(cancelConn);
        PQcancelFinish(cancelConn);
    }

    // Close the database connection
    PQfinish(conn);
}
```