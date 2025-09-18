# disconnectDatabase

## Location
src/fe_utils/connect_utils.c: 158 - 171

## Overview
Safely disconnects a PostgreSQL database connection, ensuring any active transactions are properly canceled before termination.

## Definition


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
  - PQtransactionStatus (check transaction state)
  - PQcancelCreate (create cancellation handle)
  - PQcancelBlocking (execute cancellation)
  - PQcancelFinish (cleanup cancellation handle)
  - PQfinish (close connection)
  - Assert (debug assertion)
  - PQTRANS_ACTIVE (transaction status constant)
  - PGcancelConn (cancellation connection type)
- Called from (representative examples):
  - main (in pg_amcheck - multiple locations)
  - compile_database_list
  - compile_relation_list_one_db
  - ParallelSlotsGetIdle
  - ParallelSlotsAdoptConn
  - ParallelSlotsTerminate

## Notes and Other Information
- Essential for preventing connection leaks in PostgreSQL client applications
- The cancellation mechanism ensures that long-running queries are properly terminated
- Used extensively in parallel processing scenarios where multiple connections need cleanup
- Part of the frontend utilities library for consistent connection management
- The function uses blocking cancellation, which waits for the cancel request to complete
- Located in src/fe_utils/connect_utils.c:158-171
- Critical for proper resource management in PostgreSQL client tools