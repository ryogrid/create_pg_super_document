# describeSubscriptions

## Location
src/bin/psql/describe.c: 6525 - 6658

## Overview
Lists PostgreSQL logical replication subscriptions with their properties and configuration details, implementing the psql \dRs meta-command functionality.

## Definition


## Detailed Description
The  function implements the  psql meta-command to display information about logical replication subscriptions. It provides both basic and verbose output modes, with the verbose mode showing additional configuration details that vary by PostgreSQL server version.

The function constructs a SQL query against the  system catalog and adapts the output columns based on:
1. The verbose flag parameter
2. PostgreSQL server version capabilities
3. Available subscription features in different versions

Key version-specific features displayed:
- PostgreSQL 10+: Basic subscription support (name, owner, enabled, publications)
- PostgreSQL 14+: Binary mode and streaming options
- PostgreSQL 15+: Two-phase commit, disable on error, skip LSN
- PostgreSQL 16+: Enhanced streaming modes (off/on/parallel), origin, password required, run as owner
- PostgreSQL 17+: Failover support

The function only shows subscriptions for the current database, filtering by .

## Parameters / Member Variables
- : Optional regular expression pattern to filter subscriptions by name. If NULL, all subscriptions in the current database are listed.
- : Boolean flag controlling whether to show additional configuration details beyond the basic subscription information.

## Dependencies
- Functions called/Symbols referenced:
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md)
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - termPQExpBuffer
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - lengthof
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in command.c for \dRs command processing)

## Notes and Other Information
- Requires PostgreSQL 10.0 or later (subscriptions were introduced in version 10)
- Only displays subscriptions for the current database (not cluster-wide)
- Uses dynamic column selection based on server version to avoid errors on older servers
- In verbose mode, shows extensive configuration details including:
  - Connection information (conninfo)
  - Synchronous commit settings
  - Binary transfer mode
  - Streaming configuration
  - Two-phase commit state
  - Error handling behavior
  - Security settings (password required, run as owner)
  - Failover capabilities
- Uses psql's standard query result formatting with internationalization support
- Returns boolean indicating success/failure of the operation
- Part of psql's describe.c module for \d commands