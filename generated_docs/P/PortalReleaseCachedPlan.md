# PortalReleaseCachedPlan

## Location
src/backend/utils/mmgr/portalmem.c: 310 - 330

## Overview
Releases a portal's reference to its cached plan by decrementing the cached plan's reference count and clearing the portal's cached plan pointer and statement list to prevent dangling references.

## Definition
```c
static void PortalReleaseCachedPlan(Portal portal)
```

## Detailed Description
PortalReleaseCachedPlan is a static utility function within the portal memory management system that safely releases a portal's reference to its cached plan. This function is crucial for proper memory management and reference counting in PostgreSQL's plan cache system.

When a portal holds a reference to a cached plan, it must properly release that reference when the portal no longer needs the plan. This function performs the release operation by calling ReleaseCachedPlan() and then clears both the cached plan pointer and the statement list to prevent any dangling references that could lead to crashes or memory corruption.

The function is designed to be safe to call multiple times or on portals that don't have a cached plan, as it checks for the existence of a cached plan before attempting to release it.

## Parameters / Member Variables
- `portal`: The portal whose cached plan reference should be released

## Dependencies
- Functions called/Symbols referenced:
  - ReleaseCachedPlan
  - Portal (type)
- Called from (representative examples):
  - PortalDrop
  - HoldPortal
  - AtAbort_Portals
  - AtSubAbort_Portals

## Notes and Other Information
- This is a static function, only accessible within portalmem.c
- Critical for preventing memory leaks in the plan cache system
- Clearing portal->stmts is essential as it becomes a dangling reference after releasing the cached plan
- Used during portal cleanup operations including normal portal destruction, transaction abort, and when converting portals to hold cursors
- The function is idempotent - safe to call multiple times on the same portal
- Part of the broader portal lifecycle management ensuring proper resource cleanup