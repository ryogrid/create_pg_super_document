# EnablePortalManager

## Location
[src/backend/utils/mmgr/portalmem.c:104-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L104-L129)

## Overview
Initializes the portal management module during backend startup by creating the top-level portal memory context and hash table for storing portal objects.

## Definition

```c
void
EnablePortalManager(void)
```
## Detailed Description
EnablePortalManager is a critical initialization function that sets up the portal management infrastructure in PostgreSQL. It creates two essential components: the TopPortalContext memory context for portal memory allocation and the PortalHashTable hash table for efficient portal lookup by name. This function must be called once during backend initialization before any portal operations can be performed.

The function establishes the memory management foundation for portals by creating a dedicated memory context under TopMemoryContext, ensuring proper memory lifecycle management for all portal-related allocations. It also initializes a hash table optimized for string-based portal name lookups.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate: Creates the top-level portal memory context
  - [hash_create](../h/hash_create.md): Creates the portal hash table for name-based lookups
  - [HASHCTL](../H/HASHCTL.md): Hash table control structure
  - MAX_PORTALNAME_LEN: Maximum length constant for portal names
  - [PortalHashEnt](../P/PortalHashEnt.md): Hash table entry structure for portals
  - PORTALS_PER_USER: Initial hash table size estimate
  - ALLOCSET_DEFAULT_SIZES: Default memory allocation set sizes
  - HASH_ELEM, HASH_STRINGS: Hash table creation flags

- Called from:
  - [InitPostgres](../I/InitPostgres.md): Backend initialization process

## Notes and Other Information
- Must be called exactly once during backend startup
- Creates TopPortalContext as a child of TopMemoryContext
- Uses PORTALS_PER_USER as initial hash table size estimate
- Asserts that TopPortalContext is NULL to ensure single initialization
- [Hash](../H/Hash.md) table is configured for string-based keys with portal name length limits