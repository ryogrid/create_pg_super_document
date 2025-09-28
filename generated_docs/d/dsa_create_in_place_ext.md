# dsa_create_in_place_ext

## Location
[src/backend/utils/mmgr/dsa.c:471-497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L471-L497)

## Overview
Creates a new Dynamic Shared Area (DSA) within existing shared memory space (either DSM or Postmaster-initialized memory) with extended control over segment sizing parameters.

## Definition

```c
dsa_area *
dsa_create_in_place_ext(void *place, size_t size,
						int tranche_id, dsm_segment *segment,
						size_t init_segment_size, size_t max_segment_size)
```
## Detailed Description
This function creates a DSA within pre-allocated shared memory space rather than creating a new DSM segment. It's designed for scenarios where the shared memory location is already established, whether in existing DSM segments or Postmaster-initialized memory. The function provides flexibility in memory management by allowing DSA expansion through additional DSM segments, though this can be constrained using dsa_set_size_limit().

Unlike dsa_create_ext(), this function uses DSM_HANDLE_INVALID and NULL for the segment handle and segment pointer in the create_internal() call, since it's working with existing memory. If a containing DSM segment is provided, it registers a cleanup callback to ensure proper resource management when the segment detaches.

The function requires explicit cleanup by all backends that create or attach to these areas, either through dsa_release_in_place() or automatically via DSM segment detach hooks when applicable.

## Parameters / Member Variables
- : Pointer to the existing shared memory location where the DSA will be created
- : Size of the existing shared memory space available for the DSA
- : LWLock tranche identifier for synchronization (must be provided by caller)
- : Optional pointer to containing DSM segment for automatic cleanup registration
- : Size for initial additional DSM segments if expansion is needed
- : Maximum size for additional DSM segments during expansion

## Dependencies
- Functions called/Symbols referenced:
  - [create_internal](../c/create_internal.md)
  - DSM_HANDLE_INVALID
  - [on_dsm_detach](../o/on_dsm_detach.md)
  - [dsa_on_dsm_detach_release_in_place](dsa_on_dsm_detach_release_in_place.md)
- Called from (representative examples):
  - dsa_create_in_place (wrapper function)

## Notes and Other Information
- Requires explicit cleanup by all backends using dsa_release_in_place() or DSM detach hooks
- Can work with both DSM and Postmaster-initialized memory spaces
- Expansion can be controlled with dsa_set_size_limit() to prevent additional DSM allocation
- Uses DSM_HANDLE_INVALID since no new DSM segment is created for the control object
- Registers cleanup callbacks only when a containing DSM segment is provided
- Located in src/backend/utils/mmgr/dsa.c:471-497

## Simplified Source

```c
// Simplified version of dsa_create_in_place_ext
dsa_area *
dsa_create_in_place_ext(void *place, size_t size,
                        int tranche_id, dsm_segment *segment,
                        size_t init_segment_size, size_t max_segment_size) {
    dsa_area *area;

    // Create DSA in existing shared memory space
    // Use DSM_HANDLE_INVALID and NULL since we're not creating a new segment
    area = create_internal(place, size, tranche_id,
                          DSM_HANDLE_INVALID, NULL,
                          init_segment_size, max_segment_size);

    // Register cleanup callback if a containing DSM segment was provided
    if (segment != NULL) {
        on_dsm_detach(segment, &dsa_on_dsm_detach_release_in_place,
                      PointerGetDatum(place));
    }

    return area;
}
```

Key simplifications made:
- Condensed the function structure for clarity
- Added clear comment explaining the key difference (using existing memory vs creating new segment)
- Highlighted the conditional cleanup registration
- Simplified the create_internal call presentation
- Focused on the core pattern: create in existing space → optionally register cleanup