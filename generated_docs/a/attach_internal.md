# attach_internal

## Location
[src/backend/utils/mmgr/dsa.c:1326-1376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1326-L1376)

## Overview
Internal workhorse function for attaching to an existing Dynamic Shared Area (DSA), creating a local backend interface to access a previously created shared memory area.

## Definition

```c
static dsa_area *
attach_internal(void *place, dsm_segment *segment, dsa_handle handle)
```
## Detailed Description
This function provides the core logic for attaching a backend process to an existing Dynamic Shared Area that was previously created by another process. It validates the integrity of the existing DSA by checking magic numbers and handles, then constructs a local  object that provides this backend's interface to the shared area.

The function performs safety checks to ensure the DSA hasn't been destroyed, increments the reference count to track active attachments, and sets up the segment mapping structures needed for this process to access the shared memory. It handles both regular DSM-based DSAs and in-place DSAs through the same interface.

## Parameters / Member Variables
- : Pointer to the memory location containing the existing DSA control structure
- : DSM segment containing the DSA (can be NULL for in-place DSAs)
- : DSA handle used to identify and validate the area

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - DSA_AREA_LOCK
  - ereport/ERROR
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - memset
- Called from (representative examples):
  - [dsa_attach](../d/dsa_attach.md)
  - [dsa_attach_in_place](../d/dsa_attach_in_place.md)

## Notes and Other Information
- This is a static internal function not exposed in the public DSA API
- Performs validation checks including magic number verification and handle consistency
- Prevents attachment to DSAs that have been destroyed (refcnt == 0)
- Thread-safe through LWLock acquisition on the DSA area lock
- Sets up segment mapping structures identical to those used during DSA creation
- The freed_segment_counter is synchronized during attachment to track segment lifecycle
- Critical error handling prevents corruption by rejecting invalid attachment attempts
- Supports both shared memory segments and in-place memory arrangements

## Simplified Source

```c
static dsa_area *attach_internal(void *place, dsm_segment *segment, dsa_handle handle)
{
    dsa_area_control *control = (dsa_area_control *) place;
    dsa_area *area;
    dsa_segment_map *segment_map;

    // Validate the DSA area integrity
    Assert(control->handle == handle);
    Assert(control->segment_handles[0] == handle);
    Assert(control->segment_header.magic == (DSA_SEGMENT_HEADER_MAGIC ^ handle ^ 0));

    // Build the backend-local area object
    area = palloc(sizeof(dsa_area));
    area->control = control;
    area->resowner = CurrentResourceOwner;
    memset(&area->segment_maps[0], 0, sizeof(dsa_segment_map) * DSA_MAX_SEGMENTS);
    area->high_segment_index = 0;

    // Set up the segment map for this process's mapping
    segment_map = &area->segment_maps[0];
    segment_map->segment = segment;  // NULL for in-place
    segment_map->mapped_address = place;
    segment_map->header = (dsa_segment_header *) segment_map->mapped_address;
    segment_map->fpm = (FreePageManager *)
        (segment_map->mapped_address + MAXALIGN(sizeof(dsa_area_control)));
    segment_map->pagemap = (dsa_pointer *)
        (segment_map->mapped_address + MAXALIGN(sizeof(dsa_area_control)) +
         MAXALIGN(sizeof(FreePageManager)));

    // Increment reference count safely
    LWLockAcquire(DSA_AREA_LOCK(area), LW_EXCLUSIVE);
    if (control->refcnt == 0)
    {
        // Can't attach to a destroyed DSA area
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("could not attach to dynamic shared area")));
    }
    ++control->refcnt;
    area->freed_segment_counter = area->control->freed_segment_counter;
    LWLockRelease(DSA_AREA_LOCK(area));

    return area;
}
```

This function attaches a backend process to an existing Dynamic Shared Area. It validates the area integrity, sets up local mapping structures, and safely increments the reference count to track active attachments.