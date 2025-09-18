# PQmakeEmptyPGresult

## Location
src/interfaces/libpq/fe-exec.c: 159 - 248

## Overview
Creates and initializes a new empty PGresult structure with a specified execution status, optionally copying connection-related information and events from a PGconn.

## Definition


## Detailed Description
PQmakeEmptyPGresult allocates and initializes a new PGresult structure with all fields set to their default values. The function serves as the foundation for creating result objects throughout the libpq library. When a connection is provided, it copies relevant connection properties (notice hooks, client encoding) and may copy the connection's error message for error status types. Additionally, it duplicates any registered PGEvents from the connection to the new result.

The function carefully handles memory allocation and ensures proper initialization of all PGresult fields. For error status types, it automatically copies the connection's current error message, providing consistent error reporting. The event duplication occurs last to ensure the result is in a valid state before potentially failing operations.

## Parameters / Member Variables
- : Connection object to copy properties from (can be NULL for minimal initialization)
- : ExecStatusType indicating the result status (success, error, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - pqSetResultError  
  - dupEvents
  - PQclear
- Called from (representative examples):
  - PQcopyResult
  - pqPrepareAsyncResult
  - pqInternalNotice
  - getCopyResult
  - pqParseInput3
  - getRowDescriptions

## Notes and Other Information
- Returns NULL if memory allocation fails
- The logic to copy connection error messages is documented as vestigial for internal callers but maintained for external API compatibility
- Events are copied last to ensure the result object is valid before any potential failure in event duplication
- All PGresult fields are explicitly initialized to ensure consistent state
- Memory size tracking includes the base PGresult structure size