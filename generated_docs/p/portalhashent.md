# portalhashent

## Location
[src/backend/utils/mmgr/portalmem.c:48-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L48-L51)

## Overview
A hash table entry structure used to store portal name-to-portal mappings in PostgreSQL's portal memory management system.

## Definition

```c
typedef struct portalhashent
{
	char		portalname[MAX_PORTALNAME_LEN];
	Portal		portal;
} PortalHashEnt;
```
## Detailed Description
The  structure (aliased as ) serves as the entry type for PostgreSQL's global portal hash table (). This structure enables efficient lookup of portals by name within the portal memory management system. Each entry contains both the portal name as a key and a pointer to the corresponding portal object, facilitating fast portal retrieval operations during query execution and management.

The structure is specifically designed for use with PostgreSQL's hash table implementation () and supports the portal lifecycle management including creation, lookup, and deletion operations through dedicated macros (, , ).

## Parameters / Member Variables
- `portalname[MAX_PORTALNAME_LEN]`: A character array of size  (64 bytes) that stores the portal's name as a null-terminated string, used as the hash key for portal lookup operations
- `portal`: A pointer to the  structure that represents the actual portal object containing execution state and query information
## Dependencies
- Symbols referenced:
  -  (defined as  = 64)
  -  (typedef for )
- Used by:
  -  macro at src/backend/utils/mmgr/portalmem.c:56-66
  -  macro at src/backend/utils/mmgr/portalmem.c:68-79  
  -  macro at src/backend/utils/mmgr/portalmem.c:81-89
  - Global variable  (HTAB type) for portal name-based lookups

## Notes and Other Information
- The structure is defined in src/backend/utils/mmgr/portalmem.c:48-51 as part of the portal memory management subsystem
- The portal name length is constrained by  (64 bytes), which is PostgreSQL's standard maximum length for identifiers
- This structure is primarily used internally by the portal management system and is not directly exposed to user code
- The hash table using this entry type is initialized with an estimated capacity of 16 portals per user () but can expand as needed
- Memory optimization: The portal's name pointer is set to point directly to the  field in the hash entry to avoid duplicate string storage