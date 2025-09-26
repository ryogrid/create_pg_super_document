# start_lo_xact

## Location
[src/bin/psql/large_obj.c:56-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/large_obj.c#L56-L97)

## Overview
Initiates or verifies a transaction block for large object operations, ensuring that large object commands execute within a proper transaction context as required by PostgreSQL.

## Definition
```c
static bool start_lo_xact(const char *operation, bool *own_transaction)
```

## Detailed Description
This function is a critical preparatory step for all large object operations in psql. It ensures that the required transaction context exists before attempting any large object manipulation. The function checks the current transaction status and either uses an existing transaction or starts a new one. It handles various transaction states appropriately, including error conditions where operations cannot proceed. The function sets a flag to indicate whether it started its own transaction, which is important for proper cleanup later.

## Parameters / Member Variables
- `operation`: String describing the operation being performed (used for error messages)
- `own_transaction`: Output parameter set to true if the function started a new transaction, false if using an existing one

## Dependencies
- Functions called/Symbols referenced:
  - [PQtransactionStatus](../P/PQtransactionStatus.md) (libpq function to check transaction status)
  - [PSQLexec](../P/PSQLexec.md) (psql utility function to execute SQL commands)
  - [PQclear](../P/PQclear.md) (libpq function to free result memory)
  - pg_log_error (PostgreSQL logging function)
  - PQTRANS_IDLE, PQTRANS_INTRANS, PQTRANS_INERROR (transaction status constants)
- Called from (representative examples):
  - [do_lo_export](../d/do_lo_export.md)
  - [do_lo_import](../d/do_lo_import.md)
  - [do_lo_unlink](../d/do_lo_unlink.md)

## Notes and Other Information
- Returns true on success, false on failure
- Large object operations in PostgreSQL must be performed within transaction blocks
- Function handles three main transaction states: idle (starts new transaction), in-transaction (uses existing), and error (cannot proceed)
- The own_transaction flag is crucial for determining whether to commit/rollback the transaction later
- Includes proper error handling for database connection issues and unknown transaction states
- Static function with scope limited to large_obj.c file