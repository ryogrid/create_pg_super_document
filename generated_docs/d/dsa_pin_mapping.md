# dsa_pin_mapping

## Location
src/backend/utils/mmgr/dsa.c: 635 - 670

## Overview
Pins a DSA area's memory mappings to keep the area attached until end of session or explicit detach, removing it from automatic resource owner cleanup.

## Definition
```c
void dsa_pin_mapping(dsa_area *area)
```

## Detailed Description
This function changes the lifecycle management of a DSA area by removing it from the current resource owner's control and pinning all associated DSM segment mappings. By default, DSA areas are owned by the current resource owner, which means they are automatically detached when that resource owner scope ends (such as when a transaction completes or a subtransaction rolls back).

When dsa_pin_mapping is called, it sets the area's resource owner to NULL and calls dsm_pin_mapping on all currently mapped DSM segments within the area. This ensures that the DSA area and its segments remain accessible until either the session ends or the area is explicitly detached, regardless of resource owner lifecycle events.

## Parameters / Member Variables
- `area`: Pointer to the DSA area to be pinned

## Dependencies
- Functions called/Symbols referenced:
  - dsm_pin_mapping
  - dsa_area (struct type)
- Called from (representative examples):
  - [GetSessionDsmHandle](../G/GetSessionDsmHandle.md) (src/backend/access/common/session.c:140)
  - [AttachSession](../A/AttachSession.md) (src/backend/access/common/session.c:188)
  - logicalrep_launcher_attach_dshmem (src/backend/replication/logical/launcher.c:1022, 1033)
  - init_dsm_registry (src/backend/storage/ipc/dsm_registry.c:105, 116)
  - pgstat_attach_shmem (src/backend/utils/activity/pgstat_shmem.c:229)

## Notes and Other Information
- Only operates if the area currently has a resource owner (area->resowner != NULL)
- Pins all currently mapped segments within the DSA area
- Once pinned, the DSA area will persist until session end or explicit detachment
- This is commonly used for long-lived shared data structures that need to survive transaction boundaries
- The function iterates through all segment slots up to high_segment_index and pins non-NULL segments
- After pinning, the area is no longer subject to automatic cleanup by the resource owner system
- Used extensively in PostgreSQL subsystems that require persistent shared memory areas like session management, replication, and statistics collection