# ecpg_autostart_transaction

## Location
[src/interfaces/ecpg/ecpglib/execute.c:1581-1601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L1581-L1601)

## Overview
Automatically starts a database transaction in non-autocommit mode when the connection is in an idle state.

## Definition

```c
bool
ecpg_autostart_transaction(struct statement *stmt)
```
## Detailed Description
This function is a utility within the ECPG library that ensures proper transaction management for embedded SQL statements. When operating in non-autocommit mode, it automatically initiates a transaction by executing a "begin transaction" command if the connection is currently idle. This is essential for maintaining proper transactional semantics in embedded SQL applications where transactions need to be started implicitly before executing statements.

The function checks the current transaction status of the connection and only starts a new transaction if:
1. The connection is in PQTRANS_IDLE state (no active transaction)
2. The connection is configured for non-autocommit mode

## Parameters / Member Variables
- : Pointer to a statement structure containing connection information, line number context, and compatibility settings used for error reporting and transaction management

## Dependencies
- Functions called/Symbols referenced:
  - PQtransactionStatus: Checks the current transaction status
  - PQexec: Executes the "begin transaction" command
  - ecpg_check_PQresult: Validates the result of the transaction start
  - ecpg_free_params: Cleans up parameters on error
  - PQTRANS_IDLE: Transaction status constant
- Called from (representative examples):
  - ecpg_do: Main ECPG statement execution function

## Notes and Other Information
- Returns true on success (transaction started or not needed), false on failure
- Automatically cleans up the PQresult after successful transaction start
- Part of ECPG's automatic transaction management system
- Only operates when autocommit is disabled on the connection
- Essential for maintaining ACID properties in embedded SQL applications