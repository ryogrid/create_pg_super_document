# PerformPortalClose

## Location
[src/backend/commands/portalcmds.c:214-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/portalcmds.c#L214-L262)

## Overview
PerformPortalClose implements the SQL CLOSE command to close a named cursor or all cursors when called with NULL name.

## Definition
```c
void PerformPortalClose(const char *name)
```

## Detailed Description
PerformPortalClose handles the execution of CLOSE cursor commands. It supports two modes of operation: closing a specific named cursor, or closing all cursors when the name parameter is NULL. The function validates the cursor name, retrieves the corresponding portal, and performs cleanup by dropping the portal.

The function performs these key operations:
1. Handles the special case of NULL name to close all cursors via PortalHashTableDeleteAll
2. Validates the cursor name (must not be empty)
3. Retrieves the portal associated with the cursor name using GetPortalByName
4. Validates that the portal exists and is valid
5. Drops the portal using PortalDrop, which automatically triggers PortalCleanup as a side-effect

The actual portal cleanup and resource deallocation is handled by PortalDrop, which ensures proper cleanup of executor state, memory contexts, and other associated resources.

## Parameters / Member Variables
- `name`: Const char pointer to the cursor name to close, or NULL to close all cursors

## Dependencies
- Functions called/Symbols referenced:
  - [PortalHashTableDeleteAll](PortalHashTableDeleteAll.md)
  - [GetPortalByName](../G/GetPortalByName.md)
  - PortalIsValid
  - [PortalDrop](PortalDrop.md)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- When name is NULL, all portals in the portal hash table are deleted via PortalHashTableDeleteAll
- Empty cursor names are explicitly rejected to avoid conflicts with protocol-level unnamed portals
- [PortalCleanup](PortalCleanup.md) is automatically called as a side-effect of PortalDrop if not already done
- The function reports appropriate errors for invalid or non-existent cursors
- The second parameter to PortalDrop (false) indicates this is not an error case cleanup
- This function is the primary interface for implementing SQL CLOSE commands