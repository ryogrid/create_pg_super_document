# disconnect_database

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:545-559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L545-L559)

## Overview
A utility function in pg_createsubscriber that safely closes a PostgreSQL database connection with optional error handling and immediate exit capability.

## Definition

```c
static void
disconnect_database(PGconn *conn, bool exit_on_error)
```
## Detailed Description
The  function provides a centralized way to close PostgreSQL database connections within the pg_createsubscriber utility. It wraps the libpq  function and adds error handling logic. When the  parameter is true, the function will terminate the entire program with exit code 1 after closing the connection, providing a fail-fast mechanism for unrecoverable error conditions.

The function includes an assertion to ensure that a valid connection pointer is provided, helping to catch programming errors during development.

## Parameters / Member Variables
- `*conn`: A pointer to the PGconn structure representing the database connection to be closed
- `exit_on_error`: A boolean flag that determines whether the program should exit immediately after closing the connection (true for undesired/error conditions, false for normal cleanup)
## Dependencies
- Functions called/Symbols referenced:
  - [PQfinish](../P/PQfinish.md) (libpq function for closing database connections)
  - Assert (macro for debug assertions)
  - exit (system function for program termination)

- Called from (representative examples):
  - [cleanup_objects_atexit](../c/cleanup_objects_atexit.md)
  - [get_primary_sysid](../g/get_primary_sysid.md)
  - [generate_object_name](../g/generate_object_name.md)
  - [setup_publisher](../s/setup_publisher.md)
  - [check_publisher](../c/check_publisher.md)
  - [check_subscriber](../c/check_subscriber.md)
  - [setup_subscriber](../s/setup_subscriber.md)
  - [wait_for_end_recovery](../w/wait_for_end_recovery.md)
  - [create_publication](../c/create_publication.md)
  - [create_subscription](../c/create_subscription.md)
  - [set_replication_progress](../s/set_replication_progress.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_createsubscriber.c file
- The function is extensively used throughout pg_createsubscriber for connection cleanup in both error and normal execution paths
- The exit_on_error mechanism provides a clean way to handle fatal errors without requiring complex error propagation through the call stack
- Located in src/bin/pg_basebackup/pg_createsubscriber.c:545-559