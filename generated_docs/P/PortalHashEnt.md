# PortalHashEnt

## Location
[src/backend/utils/mmgr/portalmem.c:52-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L52-L55)

## Overview
PortalHashEnt is a hash table entry structure used in PostgreSQL's portal memory management system to map portal names to their corresponding Portal objects.

## Definition
```c
typedef struct portalhashent
{
    char        portalname[MAX_PORTALNAME_LEN];
    Portal      portal;
} PortalHashEnt;
```

## Detailed Description
PortalHashEnt serves as the key-value pair structure for the global PortalHashTable, which provides fast lookup of Portal objects by name. This structure is fundamental to PostgreSQL's portal management system, allowing the system to efficiently store, retrieve, and manage named portals (prepared statements and cursors) throughout their lifecycle.

The structure is designed to work with PostgreSQL's hash table infrastructure (HTAB), where the portalname field acts as the hash key and the portal field stores the associated Portal pointer. This design enables O(1) average-case lookup performance for portal operations.

## Parameters / Member Variables
- `portalname[MAX_PORTALNAME_LEN]`: The name of the portal, used as the hash key. Limited to NAMEDATALEN characters (typically 64 bytes including null terminator)
- `portal`: Pointer to the Portal structure (PortalData) that contains the actual portal state, query information, and execution context

## Dependencies
- Functions called/Symbols referenced:
  - [Portal](Portal.md) (typedef for PortalData pointer)
  - MAX_PORTALNAME_LEN (constant, equals NAMEDATALEN)
  - [HTAB](../H/HTAB.md) (hash table type)

- Used by (representative examples):
  - PortalHashTableLookup (macro for finding portals by name)
  - PortalHashTableInsert (macro for inserting new portal entries)
  - PortalHashTableDelete (macro for removing portal entries)
  - [EnablePortalManager](../E/EnablePortalManager.md) (initializes the portal hash table)
  - Various portal lifecycle functions (PreCommit_Portals, AtAbort_Portals, etc.)

## Notes and Other Information
- This structure is part of the internal implementation of portal memory management and is not exposed to external modules
- The hash table using this structure is initialized during EnablePortalManager() and persists throughout the backend process lifetime
- [Portal](Portal.md) names must be unique within a database session, enforced through the hash table lookup mechanism
- The structure is used extensively in transaction cleanup operations to iterate through all active portals
- Memory for PortalHashEnt instances is managed by PostgreSQL's hash table infrastructure, not directly by application code