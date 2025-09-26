# make_new_segment

## Location
[src/backend/utils/mmgr/dsa.c:2081-2251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L2081-L2251)

## Overview
Creates a new dynamic shared memory segment within a DSA area with at least the requested number of pages, implementing geometric growth strategy and comprehensive size validation.

## Definition

```c
static dsa_segment_map *
make_new_segment(dsa_area *area, size_t requested_pages)
```
## Detailed Description
The  function creates a new segment to expand the available memory in a dynamic shared area. It implements a sophisticated sizing algorithm that balances several considerations:

**Geometric Growth Strategy**: Uses exponential sizing based on segment index to approximately double total storage with each new segment. The formula uses  to create power-of-two sized segments.

**Size Calculation Process**:
1. **Find available slot**: Searches linearly for an unused segment index
2. **Calculate target size**: Applies geometric growth formula with various limits
3. **Compute metadata overhead**: Accounts for segment header, free page manager, and page map
4. **Validate constraints**: Ensures the segment fits within total size limits
5. **Handle large requests**: Creates odd-sized segments when geometric sizing is insufficient

**Resource Management**: Creates the underlying DSM segment, pins it to prevent automatic cleanup, updates control structures, and initializes the free page manager.

## Parameters / Member Variables
- : Pointer to the dynamic shared area that will contain the new segment
- : Minimum number of usable pages required in the new segment

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md) / DSA_AREA_LOCK (lock assertion)
  - [dsm_create](../d/dsm_create.md)
  - [dsm_pin_segment](../d/dsm_pin_segment.md)
  - [dsm_segment_handle](../d/dsm_segment_handle.md)
  - [dsm_segment_address](../d/dsm_segment_address.md)
  - [FreePageManagerInitialize](../F/FreePageManagerInitialize.md)
  - [FreePageManagerPut](../F/FreePageManagerPut.md)
  - [contiguous_pages_to_segment_bin](../c/contiguous_pages_to_segment_bin.md)
  - [get_segment_by_index](../g/get_segment_by_index.md)
  - Various constants: DSA_MAX_SEGMENTS, FPM_PAGE_SIZE, DSA_MAX_SEGMENT_SIZE, etc.
- Called from (representative examples):
  - [dsa_allocate_extended](../d/dsa_allocate_extended.md)
  - [ensure_active_superblock](../e/ensure_active_superblock.md)

## Notes and Other Information
- This is a static (internal) function used for dynamic memory expansion
- Must be called with the DSA area lock held (enforced by assertion)
- Returns NULL if constraints cannot be satisfied (segment limit reached, size limit exceeded)
- Implements power-of-two sizing strategy for potential future huge page compatibility
- Handles both geometric growth (normal case) and exact sizing (large allocation case)
- Updates multiple tracking variables: high_segment_index, total_segment_size
- Initializes segment header with magic number for validation
- Links the new segment into the appropriate bin in the segment management system
- Part of PostgreSQL's sophisticated shared memory allocation infrastructure