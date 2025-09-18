# setup_cancel_handler

## Location
[src/fe_utils/cancel.c:232-243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/cancel.c#L232-L243)

## Overview
setup_cancel_handler initializes the cancel handling infrastructure for PostgreSQL frontend utilities, setting up signal handlers and callback mechanisms for query cancellation.

## Definition


## Detailed Description
setup_cancel_handler is a Windows-specific initialization function that configures the query cancellation system for PostgreSQL frontend utilities. It sets up the necessary infrastructure including a user-defined callback function, localized status messages, critical section synchronization, and registers the console control handler for processing Ctrl+C and Ctrl+Break events.

The function initializes global variables used by the cancel handling system, including the callback function that will be invoked when a cancellation is requested, and the messages displayed to users when cancellation succeeds or fails. It also initializes the critical section used for thread-safe access to the cancel connection and registers the consoleHandler function to handle Windows console control events.

## Parameters / Member Variables
- : A function pointer to a user-defined callback function that will be called when a cancellation is requested (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [consoleHandler](../c/consoleHandler.md)
- Called from (representative examples):
  - [main](../m/main.md) (pg_amcheck)
  - [runInitSteps](../r/runInitSteps.md) (pgbench)
  - [psql_setup_cancel_handler](../p/psql_setup_cancel_handler.md) (psql)
  - [main](../m/main.md) (clusterdb)
  - [main](../m/main.md) (reindexdb)
  - [main](../m/main.md) (vacuumdb)

## Notes and Other Information
- Windows-specific function that uses SetConsoleCtrlHandler API
- Initializes localized messages using gettext (_() macro) for internationalization support
- Sets up critical section (cancelConnLock) for thread-safe access to cancel connection data
- The callback parameter is optional and can be NULL if no custom cleanup is needed
- Must be called before using other cancel handling functions like SetCancelConn
- Part of the frontend utilities cancel mechanism used by various PostgreSQL client tools
- Registers consoleHandler to process CTRL_C_EVENT and CTRL_BREAK_EVENT signals