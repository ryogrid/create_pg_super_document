# disconnect_database

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 545 - 559

## Overview
A utility function in pg_createsubscriber that safely closes a PostgreSQL database connection with optional error handling and immediate exit capability.

## Definition


## Detailed Description
The  function provides a centralized way to close PostgreSQL database connections within the pg_createsubscriber utility. It wraps the libpq  function and adds error handling logic. When the  parameter is true, the function will terminate the entire program with exit code 1 after closing the connection, providing a fail-fast mechanism for unrecoverable error conditions.

The function includes an assertion to ensure that a valid connection pointer is provided, helping to catch programming errors during development.

## Parameters / Member Variables
- : A pointer to the PGconn structure representing the database connection to be closed
- : A boolean flag that determines whether the program should exit immediately after closing the connection (true for undesired/error conditions, false for normal cleanup)

## Dependencies
- Functions called/Symbols referenced:
  - PQfinish (libpq function for closing database connections)
  - Assert (macro for debug assertions)
  - exit (system function for program termination)

- Called from (representative examples):
  - cleanup_objects_atexit
  - get_primary_sysid
  - generate_object_name
  - setup_publisher
  - check_publisher
  - check_subscriber
  - setup_subscriber
  - wait_for_end_recovery
  - create_publication
  - create_subscription
  - set_replication_progress

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_createsubscriber.c file
- The function is extensively used throughout pg_createsubscriber for connection cleanup in both error and normal execution paths
- The exit_on_error mechanism provides a clean way to handle fatal errors without requiring complex error propagation through the call stack
- Located in src/bin/pg_basebackup/pg_createsubscriber.c:545-559