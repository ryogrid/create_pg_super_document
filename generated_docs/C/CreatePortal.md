# CreatePortal

## Location
src/backend/utils/mmgr/portalmem.c: 175 - 234

## Overview
Creates a new portal with the specified name, handling duplicate name conflicts and initializing all necessary portal structures and contexts.

## Definition
```c
Portal CreatePortal(const char *name, bool allowDup, bool dupSilent)
```

## Detailed Description
CreatePortal is the primary portal creation function that establishes a new portal object with comprehensive initialization. The function handles duplicate name detection and resolution based on the provided flags, creates all necessary memory contexts and resource management structures, and properly registers the portal in the global hash table.

The function performs several critical initialization steps: validates the portal name, checks for existing portals with the same name, allocates memory for the portal structure in TopPortalContext, creates a dedicated memory context for the portal, establishes a resource owner for cleanup tracking, initializes all portal fields with appropriate defaults, and registers the portal in the global hash table.

Portal creation involves setting up proper transaction and subtransaction tracking, establishing default execution strategies, and configuring cursor options. The function ensures that portals are properly integrated into PostgreSQL's memory management and resource cleanup systems.

## Parameters / Member Variables
- `name`: Null-terminated string containing the portal name; must be valid (not NULL)
- `allowDup`: Boolean flag controlling duplicate name handling; if true, silently replaces existing portal; if false, raises an error for duplicates  
- `dupSilent`: Boolean flag controlling warning messages; if true, suppresses duplicate portal warning messages; only effective when allowDup is true

## Dependencies
- Functions called/Symbols referenced:
  - GetPortalByName: Checks for existing portals with the same name
  - PortalIsValid: Validates portal objects
  - PortalDrop: Removes existing portals when duplicates are allowed
  - MemoryContextAllocZero: Allocates zero-initialized memory for portal structure
  - AllocSetContextCreate: Creates dedicated memory context for portal
  - ResourceOwnerCreate: Establishes resource ownership for cleanup tracking
  - GetCurrentSubTransactionId: Sets portal creation subtransaction ID
  - GetCurrentTransactionNestLevel: Sets portal creation transaction nesting level
  - GetCurrentStatementStartTimestamp: Records portal creation timestamp
  - PortalHashTableInsert: Registers portal in global hash table
  - MemoryContextSetIdentifier: Sets memory context debugging identifier
  - PortalCleanup: Default cleanup function for portals

- Called from:
  - exec_bind_message: Extended query protocol portal binding
  - exec_simple_query: Simple query protocol portal creation
  - PerformCursorOpen: SQL cursor declaration
  - SPI_cursor_open_internal: Server Programming Interface cursor operations
  - CreateNewPortal: High-level portal creation wrapper

## Notes and Other Information
- Always creates portal in TopPortalContext for proper memory lifecycle management
- Establishes dedicated PortalContext as child context using ALLOCSET_SMALL_SIZES
- Initializes portal with PORTAL_MULTI_QUERY strategy and CURSOR_OPT_NO_SCROLL options by default
- Portal starts in PORTAL_NEW status with atStart=true and atEnd=true to prevent premature fetches
- Creates resource owner as child of CurTransactionResourceOwner for proper cleanup tracking
- Records creation timestamp and transaction identifiers for debugging and transaction management
- Portal name validation ensures crashes are prevented from NULL pointer access
- Duplicate handling provides flexible conflict resolution for different usage scenarios