# PerformPortalFetch

## Location
src/backend/commands/portalcmds.c: 167 - 213

## Overview
PerformPortalFetch executes SQL FETCH or MOVE commands, retrieving a specified number of rows from a previously declared cursor in a given direction.

## Definition
```c
void PerformPortalFetch(FetchStmt *stmt, DestReceiver *dest, QueryCompletion *qc)
```

## Detailed Description
PerformPortalFetch implements the execution logic for FETCH and MOVE SQL commands. It validates the cursor name, retrieves the corresponding portal, and executes the fetch operation according to the specified direction and count. The function handles both FETCH (which returns data) and MOVE (which only advances the cursor position without returning data) operations.

The function performs these key operations:
1. Validates the cursor name (must not be empty)
2. Retrieves the portal associated with the cursor name
3. Validates that the portal exists and is valid
4. Adjusts the destination receiver for MOVE operations (uses DestNone)
5. Executes the portal fetch operation with specified direction and count
6. Updates query completion status with the number of processed rows

The actual row fetching is delegated to PortalRunFetch, which handles the complex logic of cursor positioning and data retrieval.

## Parameters / Member Variables
- `stmt`: FetchStmt containing the parsed FETCH/MOVE statement with portal name, direction, and row count
- `dest`: DestReceiver specifying where to send the fetched results (modified to DestNone for MOVE operations)
- `qc`: QueryCompletion pointer for storing command completion status (may be NULL if caller doesn't want status)

## Dependencies
- Functions called/Symbols referenced:
  - GetPortalByName
  - PortalIsValid
  - [PortalRunFetch](PortalRunFetch.md)
  - SetQueryCompletion
  - None_Receiver
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- MOVE operations use DestNone receiver to discard results while still advancing cursor position
- The function validates cursor existence and reports appropriate errors for invalid or non-existent cursors
- [Query](../Q/Query.md) completion status includes the number of rows processed, which is useful for client applications
- The actual fetch direction and count validation is handled by lower-level portal functions
- Empty cursor names are explicitly rejected to avoid conflicts with protocol-level unnamed portals