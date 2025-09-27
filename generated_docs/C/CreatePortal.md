# CreatePortal

## Location
[src/backend/utils/mmgr/portalmem.c:175-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L175-L234)

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
  - [GetPortalByName](../G/GetPortalByName.md): Checks for existing portals with the same name
  - PortalIsValid: Validates portal objects
  - [PortalDrop](../P/PortalDrop.md): Removes existing portals when duplicates are allowed
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md): Allocates zero-initialized memory for portal structure
  - AllocSetContextCreate: Creates dedicated memory context for portal
  - [ResourceOwnerCreate](../R/ResourceOwnerCreate.md): Establishes resource ownership for cleanup tracking
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md): Sets portal creation subtransaction ID
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md): Sets portal creation transaction nesting level
  - [GetCurrentStatementStartTimestamp](../G/GetCurrentStatementStartTimestamp.md): Records portal creation timestamp
  - PortalHashTableInsert: Registers portal in global hash table
  - [MemoryContextSetIdentifier](../M/MemoryContextSetIdentifier.md): Sets memory context debugging identifier
  - [PortalCleanup](../P/PortalCleanup.md): Default cleanup function for portals

- Called from:
  - [exec_bind_message](../e/exec_bind_message.md): Extended query protocol portal binding
  - [exec_simple_query](../e/exec_simple_query.md): Simple query protocol portal creation
  - [PerformCursorOpen](../P/PerformCursorOpen.md): SQL cursor declaration
  - [SPI_cursor_open_internal](../S/SPI_cursor_open_internal.md): Server Programming Interface cursor operations
  - [CreateNewPortal](CreateNewPortal.md): High-level portal creation wrapper

## Notes and Other Information
- Always creates portal in TopPortalContext for proper memory lifecycle management
- Establishes dedicated PortalContext as child context using ALLOCSET_SMALL_SIZES
- Initializes portal with PORTAL_MULTI_QUERY strategy and CURSOR_OPT_NO_SCROLL options by default
- [Portal](../P/Portal.md) starts in PORTAL_NEW status with atStart=true and atEnd=true to prevent premature fetches
- Creates resource owner as child of CurTransactionResourceOwner for proper cleanup tracking
- Records creation timestamp and transaction identifiers for debugging and transaction management
- [Portal](../P/Portal.md) name validation ensures crashes are prevented from NULL pointer access
- Duplicate handling provides flexible conflict resolution for different usage scenarios

## Simplified Source

```c
// Simplified version of CreatePortal
Portal CreatePortal(const char *name, bool allowDup, bool dupSilent) {
    Portal portal;

    // Validate input parameters
    Assert(PointerIsValid(name));

    // Check if portal with this name already exists
    portal = GetPortalByName(name);
    if (PortalIsValid(portal)) {
        // Handle duplicate portal based on flags
        if (!allowDup) {
            ereport(ERROR, "cursor already exists");
        }
        if (!dupSilent) {
            ereport(WARNING, "closing existing cursor");
        }
        PortalDrop(portal, false);
    }

    // Allocate new portal structure in TopPortalContext
    portal = (Portal) MemoryContextAllocZero(TopPortalContext, sizeof *portal);

    // Create dedicated memory context for this portal
    portal->portalContext = AllocSetContextCreate(TopPortalContext,
                                                  "PortalContext",
                                                  ALLOCSET_SMALL_SIZES);

    // Create resource owner for cleanup tracking
    portal->resowner = ResourceOwnerCreate(CurTransactionResourceOwner, "Portal");

    // Initialize portal with default values
    portal->status = PORTAL_NEW;
    portal->cleanup = PortalCleanup;
    portal->createSubid = GetCurrentSubTransactionId();
    portal->activeSubid = portal->createSubid;
    portal->createLevel = GetCurrentTransactionNestLevel();
    portal->strategy = PORTAL_MULTI_QUERY;
    portal->cursorOptions = CURSOR_OPT_NO_SCROLL;
    portal->atStart = true;
    portal->atEnd = true;  // Prevent fetches until query is set
    portal->visible = true;
    portal->creation_time = GetCurrentStatementStartTimestamp();

    // Register portal in hash table (sets portal->name)
    PortalHashTableInsert(portal, name);

    // Set memory context identifier for debugging
    MemoryContextSetIdentifier(portal->portalContext,
                              portal->name[0] ? portal->name : "<unnamed>");

    return portal;
}
```

Key simplifications made:
- Simplified error reporting to show essential logic without detailed error codes
- Consolidated duplicate portal handling logic for clarity
- Added inline comments explaining each major step
- Focused on the main execution path and core functionality
- Preserved all essential initialization steps and their relationships