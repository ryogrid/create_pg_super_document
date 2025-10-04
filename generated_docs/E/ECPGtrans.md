# ECPGtrans

## Location
[src/interfaces/ecpg/ecpglib/misc.c:160-203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/misc.c#L160-L203)

## Overview
Executes transaction control commands (BEGIN, COMMIT, ROLLBACK, etc.) on an ECPG database connection with automatic transaction management.

## Definition
```c
bool ECPGtrans(int lineno, const char *connection_name, const char *transaction)
```

## Detailed Description
ECPGtrans is a core ECPG function that handles transaction control operations. It intelligently manages transaction state by automatically starting transactions when needed (except in autocommit mode). The function handles special cases like BEGIN statements, prepared transactions (COMMIT PREPARED, ROLLBACK PREPARED), and ensures proper transaction boundaries. It logs all transaction operations and provides error handling through the ECPG error management system.

## Parameters / Member Variables
- `lineno`: Line number in the source file where the transaction command was issued (used for error reporting)
- `connection_name`: String identifier for the ECPG database connection
- `transaction`: SQL transaction command string to execute (e.g., "begin", "commit", "rollback")

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_get_connection](../e/ecpg_get_connection.md)
  - [ecpg_init](../e/ecpg_init.md)
  - [ecpg_log](../e/ecpg_log.md)
  - [PQtransactionStatus](../P/PQtransactionStatus.md)
  - PQTRANS_IDLE
  - [PQexec](../P/PQexec.md)
  - [ecpg_check_PQresult](../e/ecpg_check_PQresult.md)
  - ECPG_COMPAT_PGSQL
- Called from (representative examples):
  - Various ECPG test programs (main functions)
  - [varchar_1](../v/varchar_1.md), string functions in cursor/define tests
  - [commitTable](../c/commitTable.md) in declare tests
  - [test_thread](../t/test_thread.md) in threading tests

## Notes and Other Information
- Automatically begins transactions when needed (unless in autocommit mode)
- Special handling for BEGIN/START, COMMIT PREPARED, and ROLLBACK PREPARED commands
- Returns false on error, true on success
- Extensively used throughout ECPG test suite for transaction management
- Part of the ECPG embedded SQL interface
- Located in src/interfaces/ecpg/ecpglib/misc.c at lines 160-203

## Simplified Source

```c
bool ECPGtrans(int lineno, const char *connection_name, const char *transaction) {
    struct connection *con = ecpg_get_connection(connection_name);

    // Initialize connection
    if (!ecpg_init(con, connection_name, lineno))
        return false;

    ecpg_log("ECPGtrans on line %d: action \"%s\"; connection \"%s\"\n",
             lineno, transaction, con ? con->name : "null");

    // Execute transaction if we have a connection
    if (con && con->connection) {
        // Auto-start transaction if needed (not in autocommit, not special commands)
        if (PQtransactionStatus(con->connection) == PQTRANS_IDLE &&
            !con->autocommit &&
            strncmp(transaction, "begin", 5) != 0 &&
            strncmp(transaction, "start", 5) != 0 &&
            strncmp(transaction, "commit prepared", 15) != 0 &&
            strncmp(transaction, "rollback prepared", 17) != 0) {

            PGresult *res = PQexec(con->connection, "begin transaction");
            if (!ecpg_check_PQresult(res, lineno, con->connection, ECPG_COMPAT_PGSQL))
                return false;
            PQclear(res);
        }

        // Execute the actual transaction command
        PGresult *res = PQexec(con->connection, transaction);
        if (!ecpg_check_PQresult(res, lineno, con->connection, ECPG_COMPAT_PGSQL))
            return false;
        PQclear(res);
    }

    return true;
}
```