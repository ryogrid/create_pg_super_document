# _PG_init

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:120-142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L120-L142)

## Overview
Module initialization function that registers the libpq-based WAL receiver function table with the PostgreSQL replication system.

## Definition

```c
void
_PG_init(void)
```
## Detailed Description
The  function is the standard PostgreSQL dynamic module initialization function for the libpqwalreceiver module. This function is automatically called when the module is loaded into the PostgreSQL server process. It registers the libpq-specific implementation of WAL receiver functions by setting the global  pointer to point to the  structure.

This module provides libpq-specific implementations for PostgreSQL's WAL receiver functionality, which is used for streaming replication. The module is loaded dynamically to avoid linking the main server binary with libpq, keeping the core server dependencies minimal.

The function includes a safety check to prevent the module from being loaded multiple times, which would indicate a configuration error or programming bug.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  -  (for error reporting)
  -  (global variable from walreceiver.h)
  -  (static function table defined in this module)

- Called from (representative examples):
  - PostgreSQL dynamic module loading system (automatically invoked)
  - Referenced by  macro

## Notes and Other Information
- This function follows the standard PostgreSQL convention for dynamic module initialization
- The function is declared with  to ensure compatibility with the PostgreSQL version
- If the module is already loaded (WalReceiverFunctions is not NULL), it will throw an ERROR
- The libpqwalreceiver module is specifically designed to provide libpq-based implementations for WAL streaming replication
- This separation allows the main PostgreSQL server to remain independent of libpq while still supporting replication features

## Simplified Source

```c
void
_PG_init(void)
{
    // Prevent double loading of the module
    if (WalReceiverFunctions != NULL)
        elog(ERROR, "libpqwalreceiver already loaded");

    // Register the libpq WAL receiver function table
    WalReceiverFunctions = &PQWalReceiverFunctions;
}
```