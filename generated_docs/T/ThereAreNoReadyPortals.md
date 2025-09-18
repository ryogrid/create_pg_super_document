# ThereAreNoReadyPortals

## Location
src/backend/utils/mmgr/portalmem.c: 1171 - 1206

## Overview
Utility function that checks whether there are any portals in PORTAL_READY status in the current session.

## Definition


## Detailed Description
ThereAreNoReadyPortals is a utility function that scans through all portals in the system to determine if any are currently in the PORTAL_READY state. A portal in PORTAL_READY status is one that has been prepared and is ready to be executed or resumed, but is not currently active.

The function performs a simple scan of the global PortalHashTable and returns false as soon as it finds any portal with PORTAL_READY status. If no ready portals are found after scanning the entire table, it returns true.

This function is typically used in situations where the system needs to verify that no prepared statements or cursors are waiting to be executed before proceeding with certain operations that might conflict with portal execution.

## Parameters
None - the function takes no parameters

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
- Data types used:
  - HASH_SEQ_STATUS
  - PortalHashEnt
  - [Portal](../P/Portal.md)
- Constants used:
  - PORTAL_READY
- Called from:
  - [CopyFrom](../C/CopyFrom.md) (src/backend/commands/copyfrom.c:741)

## Notes and Other Information
- The function returns true if no ready portals exist, false if at least one ready portal is found
- This is a simple boolean check function with minimal overhead
- The function provides early termination - it stops scanning as soon as the first ready portal is found
- Used primarily for safety checks in operations that might be incompatible with pending portal execution
- The function is defined in src/backend/utils/mmgr/portalmem.c:1171-1206