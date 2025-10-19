# getTransactionStatus

## Location
[src/bin/pgbench/pgbench.c:3527-3563](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L3527-L3563)

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
  - [PQtransactionStatus](../P/PQtransactionStatus.md)
  - [PQstatus](../P/PQstatus.md)  
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
  - [advanceConnectionState](../a/advanceConnectionState.md)

## Notes and Other Information
- Returns TStatus enumeration values: TSTATUS_IDLE, TSTATUS_IN_BLOCK, TSTATUS_CONN_ERROR, or TSTATUS_OTHER_ERROR
- Critical for pgbench's decision-making in transaction management and error handling
- Includes special handling for broken connections (CONNECTION_BAD)
- Contains assertions and error logging for unexpected states that should never occur
- Used extensively in pgbench's connection state advancement logic to determine appropriate actions

## Simplified Source

```c
static TStatus getTransactionStatus(PGconn *con)
{
    PGTransactionStatusType tx_status = PQtransactionStatus(con);

    switch (tx_status)
    {
        case PQTRANS_IDLE:
            return TSTATUS_IDLE;

        case PQTRANS_INTRANS:
        case PQTRANS_INERROR:
            return TSTATUS_IN_BLOCK;

        case PQTRANS_UNKNOWN:
            // Check if connection is broken
            if (PQstatus(con) == CONNECTION_BAD)
                return TSTATUS_CONN_ERROR;
            // Fall through to error case

        case PQTRANS_ACTIVE:
        default:
            // Unexpected transaction status
            pg_log_error("unexpected transaction status %d", tx_status);
            return TSTATUS_OTHER_ERROR;
    }
}
```