# CreateNewPortal

## Location
[src/backend/utils/mmgr/portalmem.c:235-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L235-L281)

## Overview
Creates a new portal with an automatically generated unique name, ensuring no naming conflicts with existing portals.

## Definition
```c
Portal CreateNewPortal(void)
```

## Detailed Description
CreateNewPortal is a convenience wrapper around CreatePortal that automatically generates unique portal names. It uses a static counter to create sequential unnamed portal names in the format "<unnamed portal N>" where N is an incrementing number. The function ensures name uniqueness by checking for conflicts and incrementing the counter until a non-conflicting name is found.

The function maintains a static unnamed_portal_count variable that persists across calls, ensuring each generated name is unique within the backend session. It performs a collision detection loop that increments the counter and checks for existing portals with the generated name using GetPortalByName.

Once a unique name is generated, the function delegates the actual portal creation to CreatePortal with strict duplicate handling (allowDup=false, dupSilent=false), ensuring that the generated name is indeed unique.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [GetPortalByName](../G/GetPortalByName.md): Checks for existing portals with the generated name
  - [CreatePortal](CreatePortal.md): Performs the actual portal creation with the generated unique name
  - sprintf: Formats the portal name string
  - MAX_PORTALNAME_LEN: Maximum length constraint for portal names

- Called from:
  - [ExecuteQuery](../E/ExecuteQuery.md): Prepared statement execution
  - [SPI_cursor_open_internal](../S/SPI_cursor_open_internal.md): Server Programming Interface cursor operations

## Notes and Other Information
- Uses static counter to ensure unique names across backend session lifetime
- Generated names follow the pattern "<unnamed portal N>" where N starts from 1
- Performs collision detection loop to guarantee name uniqueness
- Delegates to CreatePortal with strict duplicate checking (no duplicates allowed)
- Primarily used for internal operations where explicit naming is not required
- The static counter may wrap around on very long-running sessions with many portals
- Essential for SPI and prepared statement operations that need temporary portals
- Provides thread-safe portal creation for unnamed use cases

## Simplified Source

```c
Portal CreateNewPortal(void) {
    static unsigned int unnamed_portal_count = 0;
    char portalname[MAX_PORTALNAME_LEN];

    // Generate unique portal name
    for (;;) {
        unnamed_portal_count++;
        sprintf(portalname, "<unnamed portal %u>", unnamed_portal_count);

        // Check if name is already in use
        if (GetPortalByName(portalname) == NULL) {
            break; // Found unique name
        }
    }

    // Create portal with generated unique name
    return CreatePortal(portalname, false, false);
}
```