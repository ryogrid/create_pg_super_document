# dsa_release_in_place

## Location
[src/backend/utils/mmgr/dsa.c:605-634](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L605-L634)

## Overview
Releases a DSA area that was created with dsa_create_in_place or dsa_attach_in_place by decrementing its reference count and unpinning associated DSM segments when the count reaches zero.

## Definition
```c
void dsa_release_in_place(void *place)
```

## Detailed Description
This function releases a DSA area located at a specific memory address by managing its reference count and associated DSM segments. When called, it decrements the area's reference count under exclusive lock protection. If the reference count reaches zero, indicating no more processes are using the area, it iterates through all associated DSM segments and unpins them from memory.

The function performs several safety checks including magic number validation and reference count assertion to ensure the area is in a valid state before performing the release operation. It is the core cleanup function used by both callback functions (dsa_on_dsm_detach_release_in_place and dsa_on_shmem_exit_release_in_place) as well as being called automatically for regular DSA areas.

## Parameters / Member Variables
- `place`: Pointer to the memory location where the DSA area control structure is located

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire
  - LWLockRelease
  - dsm_unpin_segment
  - dsa_area_control (struct type)
  - DSA_SEGMENT_HEADER_MAGIC
  - DSM_HANDLE_INVALID
- Called from (representative examples):
  - pgstat_detach_shmem (src/backend/utils/activity/pgstat_shmem.c:255)
  - [dsa_on_dsm_detach_release_in_place](dsa_on_dsm_detach_release_in_place.md) (src/backend/utils/mmgr/dsa.c:578)
  - [dsa_on_shmem_exit_release_in_place](dsa_on_shmem_exit_release_in_place.md) (src/backend/utils/mmgr/dsa.c:592)

## Notes and Other Information
- It is preferable to use the dsa_on_XXX callbacks for automatic management rather than calling this directly
- Failure to release an in-place area leaks its segments permanently
- The function is also called automatically for areas created with dsa_create or dsa_attach as an implementation detail
- Uses exclusive locking to ensure thread-safe reference counting
- Validates area integrity using magic number checks before performing operations
- Only unpins DSM segments when the reference count reaches zero to prevent premature cleanup
- The function handles arrays of segment handles and only unpins valid (non-invalid) handles