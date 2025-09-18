# _internalPQconninfoOption

## Location
src/interfaces/libpq/fe-connect.c: 168 - 187

## Overview
An internal structure that extends PQconninfoOption with additional private fields for managing PostgreSQL connection parameters within libpq.

## Definition


## Detailed Description
This structure is the internal representation of connection information options used by libpq for managing PostgreSQL database connections. It extends the public PQconninfoOption structure with an additional private field () that tracks the offset of each option within the PGconn structure. The first part of this structure is intentionally kept synchronized with PQconninfoOption in libpq-fe.h to allow safe memory copying between the two structures. This design allows libpq to maintain both a public API and internal implementation details while ensuring compatibility.

The structure serves as the foundation for connection parameter management, supporting fallback mechanisms through environment variables and compiled-in defaults, and providing metadata for GUI applications that want to create database connection dialogs.

## Parameters / Member Variables
- : The name/keyword of the connection option (e.g., "host", "port", "dbname")
- : Name of the environment variable to check for fallback value (e.g., "PGHOST", "PGPORT")
- : Compiled-in default value used when no other value is available
- : Current value of the option, or NULL if not set
- : Human-readable label for the option, used in connection dialogs
- : Display character indicator for GUI applications:
  - : Normal input field
  - : Password field (hide value)
  - : Debug option (don't show by default)
- : Suggested field size in characters for dialog display
- : Offset into the PGconn structure where this option's value is stored, or -1 if not stored there

## Dependencies
- Functions called/Symbols referenced:
  - [PQconninfoOption](../P/PQconninfoOption.md) (public counterpart structure)
  - PGconn (connection structure where values are stored)
- Called from (representative examples):
  - Connection parameter processing functions
  - PQconndefaults() and related functions

## Notes and Other Information
- Critical synchronization requirement: The first 7 fields must remain identical to PQconninfoOption in libpq-fe.h
- Memory management: Non-null  fields point to malloc'd strings that must be freed appropriately
- The  field enables efficient mapping between connection options and the actual PGconn structure fields
- Used internally by libpq for processing connection strings, environment variables, and default values
- Located in 