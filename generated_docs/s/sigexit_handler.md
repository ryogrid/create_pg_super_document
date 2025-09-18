# sigexit_handler

## Location
[src/bin/pg_basebackup/pg_recvlogical.c:674-683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_recvlogical.c#L674-L683)

## Overview
A signal handler function that sets a global flag to gracefully stop the WAL receiver process when termination signals are received.

## Definition


## Detailed Description
The  function is a simple signal handler designed to provide graceful termination for PostgreSQL WAL (Write-Ahead Log) receiver utilities. When termination signals (such as SIGTERM or SIGINT) are received, this handler sets the global boolean variable  to , which allows the main processing loop to detect the termination request and exit cleanly. This approach ensures that the process can complete any ongoing operations and perform proper cleanup before shutting down.

## Parameters / Member Variables
- : Standard PostgreSQL macro for signal handler arguments, typically expands to  representing the signal number

## Dependencies
- Functions called/Symbols referenced:
  - SIGNAL_ARGS (macro)
  - time_to_stop (global variable)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_receivewal.c)
  - [main](../m/main.md) (in pg_recvlogical.c)

## Notes and Other Information
- This is a static function, meaning it's only visible within its compilation unit
- The handler is registered for termination signals in the main function of WAL receiver utilities
- The simple design (just setting a flag) makes it signal-safe, avoiding potential issues with more complex operations in signal handlers
- Used in both pg_receivewal and pg_recvlogical utilities for consistent termination handling