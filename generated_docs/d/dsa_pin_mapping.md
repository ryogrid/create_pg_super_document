# dsa_pin_mapping

## Location
[src/backend/utils/mmgr/dsa.c:635-670](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L635-L670)

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
  - [dsm_pin_mapping](dsm_pin_mapping.md)
  - [dsa_area](dsa_area.md) (struct type)
- Called from (representative examples):
  - [GetSessionDsmHandle](../G/GetSessionDsmHandle.md) (src/backend/access/common/session.c:140)
  - [AttachSession](../A/AttachSession.md) (src/backend/access/common/session.c:188)
  - [logicalrep_launcher_attach_dshmem](../l/logicalrep_launcher_attach_dshmem.md) (src/backend/replication/logical/launcher.c:1022, 1033)
  - [init_dsm_registry](../i/init_dsm_registry.md) (src/backend/storage/ipc/dsm_registry.c:105, 116)
  - [pgstat_attach_shmem](../p/pgstat_attach_shmem.md) (src/backend/utils/activity/pgstat_shmem.c:229)

## Notes and Other Information
- Only operates if the area currently has a resource owner (area->resowner != NULL)
- Pins all currently mapped segments within the DSA area
- Once pinned, the DSA area will persist until session end or explicit detachment
- This is commonly used for long-lived shared data structures that need to survive transaction boundaries
- The function iterates through all segment slots up to high_segment_index and pins non-NULL segments
- After pinning, the area is no longer subject to automatic cleanup by the resource owner system
- Used extensively in PostgreSQL subsystems that require persistent shared memory areas like session management, replication, and statistics collection

## Simplified Source

```c
// Simplified version of dsa_pin_mapping
void dsa_pin_mapping(dsa_area *area) {
    int i;

    // Only proceed if area has a resource owner
    if (area->resowner != NULL) {
        // Remove from resource owner control - makes it persistent
        area->resowner = NULL;

        // Pin all currently mapped segments in the area
        for (i = 0; i <= area->high_segment_index; ++i) {
            if (area->segment_maps[i].segment != NULL) {
                dsm_pin_mapping(area->segment_maps[i].segment);
            }
        }
    }
}
```

Key simplifications made:
- Preserved the original logic structure as it's already quite clean and readable
- Added explanatory comments to clarify the purpose of each step
- Maintained the essential algorithm: check resource owner, clear it, then pin all mapped segments
- No major simplifications needed as the original function is already concise and straightforward