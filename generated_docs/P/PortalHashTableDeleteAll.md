# PortalHashTableDeleteAll

## Location
[src/backend/utils/mmgr/portalmem.c:607-635](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L607-L635)

## Overview
Deletes all declared cursors/portals from the portal hash table, used to implement the CLOSE ALL and DISCARD ALL SQL commands.

## Definition
```c
void PortalHashTableDeleteAll(void)
```

## Detailed Description
PortalHashTableDeleteAll provides a mechanism to close all existing portals in the system, which is essential for implementing SQL commands like CLOSE ALL and DISCARD ALL. The function iterates through the entire portal hash table and drops each portal, except for any portal that is currently active (executing).

The function uses a careful iteration strategy that restarts the hash table scan after each portal drop. This is necessary because dropping a portal may trigger cascading drops of other portals, which would invalidate the current iteration state. By restarting the iteration after each drop, the function ensures all portals are properly handled without encountering invalid hash table states.

The function safely handles the case where no portal hash table exists by returning early, making it robust for use in various system states.

## Parameters / Member Variables
None - this function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md) (hash table iteration status structure)
  - [PortalHashEnt](PortalHashEnt.md) (portal hash table entry structure)  
  - [hash_seq_init](../h/hash_seq_init.md) (initialize hash table iteration)
  - [hash_seq_search](../h/hash_seq_search.md) (get next entry in hash table iteration)
  - [Portal](Portal.md) (portal structure type)
  - PORTAL_ACTIVE (constant for active portal state)
  - [PortalDrop](PortalDrop.md) (function to drop individual portals)
  - [hash_seq_term](../h/hash_seq_term.md) (terminate hash table iteration)
- Called from (representative examples):
  - [DiscardAll](../D/DiscardAll.md) (src/backend/commands/discard.c:69)
  - [PerformPortalClose](PerformPortalClose.md) (src/backend/commands/portalcmds.c:221)
  - PortalIsValid (src/include/utils/portal.h:247)

## Notes and Other Information
- Implements the backend functionality for SQL commands CLOSE ALL and DISCARD ALL
- Skips active portals to prevent closing the portal that is currently executing the command
- Uses a restart-iteration strategy to handle potential cascading portal drops safely
- The function is safe to call even when the portal hash table doesn't exist
- Each portal drop uses isTopCommit=false, treating each drop as a non-top-level operation
- Located in src/backend/utils/mmgr/portalmem.c:607-635