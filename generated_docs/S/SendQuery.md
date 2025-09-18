# SendQuery

## Location
[src/bin/psql/common.c:1082-1313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L1082-L1313)

## Overview
SendQuery is the main "front door" function in psql for sending user-entered queries to the PostgreSQL backend, handling transaction management, error rollback, and result processing.

## Definition


## Detailed Description
SendQuery serves as the primary interface for executing queries entered directly by users in psql. It provides comprehensive query execution management including:

- **Single-step mode verification**: When enabled, prompts user to confirm query execution
- **Echo handling**: Displays queries based on echo settings (PSQL_ECHO_QUERIES, PSQL_ECHO_ERRORS)
- **Transaction management**: Automatically begins transactions when autocommit is off and handles savepoints for error rollback
- **Query execution**: Routes to either DescribeQuery (for \gdesc) or ExecQueryAndProcessResults for normal execution
- **Error handling**: Implements sophisticated error rollback using savepoints when configured
- **Timing output**: Records and displays query execution time when timing is enabled
- **State cleanup**: Performs comprehensive cleanup of various psql state variables after execution

The function distinguishes itself from PSQLexec() by being designed for user-facing queries that are subject to single-step mode and full transaction management.

## Parameters / Member Variables
- : The SQL query string to be executed

## Dependencies
- Functions called/Symbols referenced:
  - PQtransactionStatus
  - [command_no_begin](../c/command_no_begin.md)
  - [PQexec](../P/PQexec.md)
  - [DescribeQuery](../D/DescribeQuery.md)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md)
  - [SetCancelConn](SetCancelConn.md)/ResetCancelConn
  - [PQclientEncoding](../P/PQclientEncoding.md)
  - [PrintTiming](../P/PrintTiming.md)
  - [PrintNotifications](../P/PrintNotifications.md)
  - [ClearOrSaveResult](../C/ClearOrSaveResult.md)
  - [clean_bind_state](../c/clean_bind_state.md)
- Called from (representative examples):
  - [MainLoop](../M/MainLoop.md) (in mainloop.c)
  - [do_copy](../d/do_copy.md) (in copy.c)
  - [ExecQueryTuples](../E/ExecQueryTuples.md) (in common.c)

## Notes and Other Information
- Returns true if query executed successfully, false otherwise
- Uses temporary savepoints (pg_psql_temporary_savepoint) for error rollback when configured
- Handles encoding changes that may occur during query execution
- Performs extensive cleanup of psql state variables including \g, \gset, \gdesc, \gexec, and \crosstabview flags
- Connected to database check is performed before execution
- Implements PostgreSQL's autocommit behavior and transaction state management