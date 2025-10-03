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
- `*area`: Pointer to the dynamic shared area that will contain the new segment
- `requested_pages`: Minimum number of usable pages required in the new segment
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

## Simplified Source

```c
static dsa_segment_map *
make_new_segment(dsa_area *area, size_t requested_pages)
{
    dsa_segment_index new_index;
    size_t metadata_bytes, total_size, total_pages, usable_pages;
    dsa_segment_map *segment_map;
    dsm_segment *segment;
    ResourceOwner oldowner;

    Assert(LWLockHeldByMe(DSA_AREA_LOCK(area)));

    // Find first available segment slot
    for (new_index = 1; new_index < DSA_MAX_SEGMENTS; ++new_index) {
        if (area->control->segment_handles[new_index] == DSM_HANDLE_INVALID)
            break;
    }
    if (new_index == DSA_MAX_SEGMENTS)
        return NULL;  // No slots available

    // Check if total size limit already exceeded
    if (area->control->total_segment_size >= area->control->max_total_segment_size)
        return NULL;

    // Calculate segment size using geometric growth
    total_size = area->control->init_segment_size *
        ((size_t) 1 << (new_index / DSA_NUM_SEGMENTS_AT_EACH_SIZE));
    total_size = Min(total_size, area->control->max_segment_size);
    total_size = Min(total_size,
                     area->control->max_total_segment_size -
                     area->control->total_segment_size);

    total_pages = total_size / FPM_PAGE_SIZE;

    // Calculate metadata overhead
    metadata_bytes = MAXALIGN(sizeof(dsa_segment_header)) +
                     MAXALIGN(sizeof(FreePageManager)) +
                     sizeof(dsa_pointer) * total_pages;

    // Round up to page boundary
    if (metadata_bytes % FPM_PAGE_SIZE != 0)
        metadata_bytes += FPM_PAGE_SIZE - (metadata_bytes % FPM_PAGE_SIZE);

    if (total_size <= metadata_bytes)
        return NULL;

    usable_pages = (total_size - metadata_bytes) / FPM_PAGE_SIZE;

    // If geometric size insufficient, calculate exact size needed
    if (requested_pages > usable_pages) {
        usable_pages = requested_pages;
        metadata_bytes = MAXALIGN(sizeof(dsa_segment_header)) +
                         MAXALIGN(sizeof(FreePageManager)) +
                         usable_pages * sizeof(dsa_pointer);

        if (metadata_bytes % FPM_PAGE_SIZE != 0)
            metadata_bytes += FPM_PAGE_SIZE - (metadata_bytes % FPM_PAGE_SIZE);

        total_size = metadata_bytes + usable_pages * FPM_PAGE_SIZE;

        // Check size limits
        if (total_size > DSA_MAX_SEGMENT_SIZE ||
            total_size > area->control->max_total_segment_size -
                         area->control->total_segment_size)
            return NULL;
    }

    // Create the DSM segment
    oldowner = CurrentResourceOwner;
    CurrentResourceOwner = area->resowner;
    segment = dsm_create(total_size, 0);
    CurrentResourceOwner = oldowner;

    if (segment == NULL)
        return NULL;

    dsm_pin_segment(segment);

    // Update control structures
    area->control->segment_handles[new_index] = dsm_segment_handle(segment);
    if (area->control->high_segment_index < new_index)
        area->control->high_segment_index = new_index;
    if (area->high_segment_index < new_index)
        area->high_segment_index = new_index;
    area->control->total_segment_size += total_size;

    // Initialize segment map
    segment_map = &area->segment_maps[new_index];
    segment_map->segment = segment;
    segment_map->mapped_address = dsm_segment_address(segment);
    segment_map->header = (dsa_segment_header *) segment_map->mapped_address;
    segment_map->fpm = (FreePageManager *)
        (segment_map->mapped_address + MAXALIGN(sizeof(dsa_segment_header)));
    segment_map->pagemap = (dsa_pointer *)
        (segment_map->mapped_address + MAXALIGN(sizeof(dsa_segment_header)) +
         MAXALIGN(sizeof(FreePageManager)));

    // Initialize free page manager
    FreePageManagerInitialize(segment_map->fpm, segment_map->mapped_address);
    FreePageManagerPut(segment_map->fpm, metadata_bytes / FPM_PAGE_SIZE, usable_pages);

    // Initialize segment header and link into bin list
    segment_map->header->magic = DSA_SEGMENT_HEADER_MAGIC ^
                                 area->control->handle ^ new_index;
    segment_map->header->usable_pages = usable_pages;
    segment_map->header->size = total_size;
    segment_map->header->bin = contiguous_pages_to_segment_bin(usable_pages);
    segment_map->header->prev = DSA_SEGMENT_INDEX_NONE;
    segment_map->header->next = area->control->segment_bins[segment_map->header->bin];
    segment_map->header->freed = false;

    area->control->segment_bins[segment_map->header->bin] = new_index;

    if (segment_map->header->next != DSA_SEGMENT_INDEX_NONE) {
        dsa_segment_map *next = get_segment_by_index(area, segment_map->header->next);
        next->header->prev = new_index;
    }

    return segment_map;
}
```