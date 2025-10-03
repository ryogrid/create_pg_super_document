# dsa_attach_in_place

## Location
[src/backend/utils/mmgr/dsa.c:545-575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L545-L575)

## Overview
Attaches to a DSA (Dynamic Shared Area) that was created with dsa_create_in_place, allowing processes to connect to memory areas created in specific memory locations.

## Definition

```c
dsa_area *
dsa_attach_in_place(void *place, dsm_segment *segment)
```
## Detailed Description
This function attaches to an existing DSA that was created using dsa_create_in_place. Unlike regular DSA attachment which uses handles, this function requires the caller to provide the exact memory location where the area was originally created. The area may be mapped at a different virtual address in the current process, but the underlying memory location must be accessible.

The function internally calls attach_internal() with the provided memory location and sets up automatic cleanup if a containing DSM segment is provided. When a DSM segment is specified, the function registers a detach callback that will automatically release the in-place area when the segment detaches.

## Parameters / Member Variables
- `*place`: Pointer to the memory location where the DSA was created with dsa_create_in_place
- `*segment`: Optional DSM segment that contains the memory area. If provided, enables automatic cleanup when the segment detaches
## Dependencies
- Functions called/Symbols referenced:
  - [attach_internal](../a/attach_internal.md)
  - [on_dsm_detach](../o/on_dsm_detach.md)
  - [dsa_on_dsm_detach_release_in_place](dsa_on_dsm_detach_release_in_place.md)
  - DSA_HANDLE_INVALID
- Called from (representative examples):
  - [AttachSession](../A/AttachSession.md) (src/backend/access/common/session.c:174)
  - [ParallelQueryMain](../P/ParallelQueryMain.md) (src/backend/executor/execParallel.c:1434)
  - [pgstat_attach_shmem](../p/pgstat_attach_shmem.md) (src/backend/utils/activity/pgstat_shmem.c:227)

## Notes and Other Information
- The caller must somehow know the exact memory location that was used when the area was created
- The memory area may be mapped at different virtual addresses across processes
- Providing the optional 'segment' parameter enables automatic cleanup when the containing DSM segment detaches
- This function is typically used for DSA areas that need to persist in specific memory locations, such as shared memory segments
- The function returns a dsa_area pointer that can be used for subsequent DSA operations

## Simplified Source

```c
// Simplified version of dsa_attach_in_place
dsa_area *
dsa_attach_in_place(void *place, dsm_segment *segment)
{
    dsa_area *area;

    // Attach to the DSA using the provided memory location
    area = attach_internal(place, NULL, DSA_HANDLE_INVALID);

    // Set up automatic cleanup if a DSM segment was provided
    if (segment != NULL) {
        on_dsm_detach(segment, &dsa_on_dsm_detach_release_in_place,
                      PointerGetDatum(place));
    }

    return area;
}
```

Key simplifications made:
- Preserved the core logic: attaching via attach_internal() and setting up cleanup
- Added clear comments explaining each major step
- Maintained the essential conditional logic for DSM segment cleanup
- Removed detailed documentation comments (preserved in main documentation)
- Function is already quite simple, so minimal changes were needed