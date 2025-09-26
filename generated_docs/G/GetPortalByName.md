# GetPortalByName

## Location
[src/backend/utils/mmgr/portalmem.c:130-150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L130-L150)

## Overview
Retrieves a portal object by its name from the global portal hash table, returning NULL if the portal is not found.

## Definition
```c
Portal GetPortalByName(const char *name)
```

## Detailed Description
GetPortalByName is a fundamental portal lookup function that searches the global PortalHashTable for a portal with the specified name. It performs a safe lookup by first validating the input name pointer before attempting the hash table search. This function is the primary mechanism for retrieving existing portals throughout PostgreSQL's query processing pipeline.

The function uses the PortalHashTableLookup macro to perform an efficient hash-based lookup. If the name parameter is NULL or invalid, the function safely returns NULL without attempting the lookup, preventing potential crashes from invalid pointer access.

## Parameters / Member Variables
- `name`: Null-terminated string containing the portal name to search for; if NULL or invalid, the function returns NULL

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid: Validates the input name pointer
  - PortalHashTableLookup: Macro for hash table lookup by portal name
  - Portal: Return type representing a portal structure

- Called from (representative examples):
  - exec_simple_query: Query execution in simple query protocol
  - exec_execute_message: Extended query protocol execution
  - PerformPortalFetch: Portal fetch operations
  - PerformPortalClose: Portal cleanup operations
  - CreatePortal: Portal creation when checking for name conflicts
  - SPI_cursor_find: Server Programming Interface cursor lookup

## Notes and Other Information
- Returns NULL for both non-existent portals and invalid name parameters
- Thread-safe lookup using the global PortalHashTable
- Widely used across PostgreSQL's query processing, cursor management, and SPI subsystems
- The PortalHashTableLookup macro handles the actual hash table interaction
- Critical for portal lifecycle management and avoiding duplicate portal names