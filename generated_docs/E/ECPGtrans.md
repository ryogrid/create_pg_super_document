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
  - ecpg_get_connection
  - [ecpg_init](../e/ecpg_init.md)
  - [ecpg_log](../e/ecpg_log.md)
  - PQtransactionStatus
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