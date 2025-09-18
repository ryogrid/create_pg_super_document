# getTransactionStatus

## Location
src/bin/pgbench/pgbench.c: 3527 - 3563

## Overview
Determines the current transaction status of a PostgreSQL connection, specifically checking if the connection is in a transaction block or error state.

## Definition
static TStatus getTransactionStatus(PGconn *con)

## Detailed Description
This function queries the PostgreSQL connection's transaction status and maps it to pgbench's internal TStatus enumeration. It serves as a critical component for transaction state management in pgbench, allowing the benchmarking tool to make informed decisions about transaction handling, rollbacks, and error recovery.

The function handles various PostgreSQL transaction states:
- IDLE: Connection is ready for new transactions
- IN TRANSACTION/IN ERROR: Connection is within a transaction block (normal or failed)
- UNKNOWN: Connection status is unclear (typically indicates connection problems)
- ACTIVE: Query is currently being processed (unexpected in this context)

## Parameters / Member Variables
- : Pointer to PGconn structure representing the PostgreSQL database connection

## Dependencies
- Functions called/Symbols referenced:
  - PQtransactionStatus
  - PQstatus  
  - pg_log_error
- Types referenced:
  - PGTransactionStatusType
  - PQTRANS_IDLE
  - PQTRANS_INTRANS
  - PQTRANS_INERROR
  - PQTRANS_UNKNOWN
  - PQTRANS_ACTIVE
  - CONNECTION_BAD
- Called from (representative examples):
  - advanceConnectionState

## Notes and Other Information
- Returns TStatus enumeration values: TSTATUS_IDLE, TSTATUS_IN_BLOCK, TSTATUS_CONN_ERROR, or TSTATUS_OTHER_ERROR
- Critical for pgbench's decision-making in transaction management and error handling
- Includes special handling for broken connections (CONNECTION_BAD)
- Contains assertions and error logging for unexpected states that should never occur
- Used extensively in pgbench's connection state advancement logic to determine appropriate actions