# PQtransactionStatus

## Location
src/interfaces/libpq/fe-connect.c: 7114 - 7123

## Overview
PQtransactionStatus returns the current transaction status of a PostgreSQL database connection, indicating whether the connection is idle, has an active command, or is within a transaction block.

## Definition
```c
PGTransactionStatusType PQtransactionStatus(const PGconn *conn)
```

## Detailed Description
This function determines the current transaction state of a PostgreSQL connection by examining both the connection status and asynchronous operation status. It provides essential information about whether the connection is available for new commands, has a command in progress, or is within a transaction block.

The function performs several checks: first ensuring the connection is valid and in CONNECTION_OK state, then checking if any asynchronous operations are active, and finally returning the stored transaction status. This layered approach ensures accurate status reporting in all connection states.

## Parameters / Member Variables
- `conn`: A pointer to a PGconn structure representing the database connection. If NULL or the connection is not in CONNECTION_OK state, the function returns PQTRANS_UNKNOWN.

## Dependencies
- Functions called/Symbols referenced:
  - CONNECTION_OK (ConnStatusType enum value)
  - PQTRANS_UNKNOWN (PGTransactionStatusType enum value)  
  - PGASYNC_IDLE (async status check)
  - PQTRANS_ACTIVE (PGTransactionStatusType enum value)
- Called from (representative examples):
  - DisconnectDatabase (pg_dump utilities)
  - getTransactionStatus (pgbench)
  - SendQuery (psql)
  - start_lo_xact (psql large object operations)
  - ECPGsetcommit (ECPG interface)
  - ecpg_autostart_transaction (ECPG)
  - ECPGtransactionStatus (ECPG)

## Notes and Other Information
- Returns PQTRANS_UNKNOWN if connection is NULL or not in CONNECTION_OK state
- Returns PQTRANS_ACTIVE if any asynchronous operation is in progress
- The PGTransactionStatusType enum includes:
  - PQTRANS_IDLE: Connection idle, ready for commands
  - PQTRANS_ACTIVE: Command currently in progress  
  - PQTRANS_INTRANS: Idle within a transaction block
  - PQTRANS_INERROR: Idle within a failed transaction block
  - PQTRANS_UNKNOWN: Cannot determine transaction status
- Essential for transaction management and determining when it is safe to issue new commands
- Widely used by PostgreSQL client tools and interfaces for proper transaction handling
- Critical for applications that need to understand transaction state before issuing commands