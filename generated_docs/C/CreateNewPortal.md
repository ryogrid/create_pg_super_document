# CreateNewPortal

## Location
src/backend/utils/mmgr/portalmem.c: 235 - 281

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
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - GetPortalByName: Checks for existing portals with the generated name
  - CreatePortal: Performs the actual portal creation with the generated unique name
  - sprintf: Formats the portal name string
  - MAX_PORTALNAME_LEN: Maximum length constraint for portal names

- Called from:
  - ExecuteQuery: Prepared statement execution
  - SPI_cursor_open_internal: Server Programming Interface cursor operations

## Notes and Other Information
- Uses static counter to ensure unique names across backend session lifetime
- Generated names follow the pattern "<unnamed portal N>" where N starts from 1
- Performs collision detection loop to guarantee name uniqueness
- Delegates to CreatePortal with strict duplicate checking (no duplicates allowed)
- Primarily used for internal operations where explicit naming is not required
- The static counter may wrap around on very long-running sessions with many portals
- Essential for SPI and prepared statement operations that need temporary portals
- Provides thread-safe portal creation for unnamed use cases