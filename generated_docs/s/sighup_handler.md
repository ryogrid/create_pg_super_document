# sighup_handler

## Location
[src/bin/pg_basebackup/pg_recvlogical.c:684-691](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_recvlogical.c#L684-L691)

## Overview
A SIGHUP signal handler that triggers the reopening of output files in the PostgreSQL logical replication receiver utility.

## Definition

```c
static void
sighup_handler(SIGNAL_ARGS)
```
## Detailed Description
The  function is a signal handler specifically designed to handle the SIGHUP signal in the pg_recvlogical utility. When a SIGHUP signal is received, this handler sets the global boolean variable  to . This mechanism allows the main processing loop to detect that output files should be reopened, which is commonly used for log rotation scenarios. The SIGHUP signal is a standard Unix signal often used to instruct long-running processes to reload their configuration or reopen files without completely restarting.

## Parameters / Member Variables
- : Standard PostgreSQL macro for signal handler arguments, typically expands to  representing the signal number

## Dependencies
- Functions called/Symbols referenced:
  - SIGNAL_ARGS (macro)
  - output_reopen (global variable)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_recvlogical.c)

## Notes and Other Information
- This is a static function, meaning it's only visible within its compilation unit
- Specifically used in the pg_recvlogical utility for logical replication WAL streaming
- The handler provides a signal-safe way to request file reopening without interrupting the main processing
- Commonly used in conjunction with log rotation tools that send SIGHUP after rotating log files
- The simple flag-setting approach ensures signal safety and avoids complex operations within the signal handler context