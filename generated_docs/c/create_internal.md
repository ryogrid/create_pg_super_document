# create_internal

## Location
[src/backend/utils/mmgr/dsa.c:1218-1325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1218-L1325)

## Overview
Internal workhorse function for creating Dynamic Shared Area (DSA) objects, handling the initialization of DSA control structures, memory management, and segment mapping for both  and  operations.

## Definition

```c
static dsa_area *
create_internal(void *place, size_t size,
				int tranche_id,
				dsm_handle control_handle,
				dsm_segment *control_segment,
				size_t init_segment_size, size_t max_segment_size)
```
## Detailed Description
This function performs the complete initialization of a Dynamic Shared Area (DSA) by setting up the control structures, memory management infrastructure, and segment mapping. It creates both the shared control structure () that coordinates access across multiple processes and the local area object () that provides this backend's interface to the shared area.

The function calculates usable memory space after accounting for metadata overhead, initializes the free page manager for memory allocation within the DSA, sets up lightweight locks for concurrent access control, and establishes the initial segment mapping. It validates input parameters against DSA size limits and ensures proper alignment of internal structures.

## Parameters / Member Variables
- : Pointer to the memory location where the DSA will be created
- : Total size of the memory area available for the DSA
- : LWLock tranche identifier for the DSA's locks
- : DSM handle for the control segment
- : DSM segment containing the DSA control structure
- : Initial size for additional segments (minimum DSA_MIN_SEGMENT_SIZE)
- : Maximum size for any segment (maximum DSA_MAX_SEGMENT_SIZE)

## Dependencies
- Functions called/Symbols referenced:
  - [dsa_minimum_size](../d/dsa_minimum_size.md)
  - [FreePageManagerInitialize](../F/FreePageManagerInitialize.md)
  - [FreePageManagerPut](../F/FreePageManagerPut.md)
  - [LWLockInitialize](../L/LWLockInitialize.md)
  - DSA_SCLASS_LOCK
  - [contiguous_pages_to_segment_bin](contiguous_pages_to_segment_bin.md)
  - [palloc](../p/palloc.md)
  - elog/ERROR
- Called from (representative examples):
  - [dsa_create_ext](../d/dsa_create_ext.md)
  - [dsa_create_in_place_ext](../d/dsa_create_in_place_ext.md)

## Notes and Other Information
- This is a static internal function not exposed in the public DSA API
- Validates that provided size meets minimum DSA requirements via dsa_minimum_size()
- Initializes all segment bins to DSA_SEGMENT_INDEX_NONE initially
- Sets up both the main DSA lock and individual size class locks for concurrent access
- The function handles both shared memory and in-place memory scenarios through the same interface
- Metadata overhead includes space for dsa_area_control, FreePageManager, and page mapping structures
- Critical for establishing the foundation of PostgreSQL's dynamic shared memory allocation system

## Simplified Source

```c
// Simplified version of create_internal
static dsa_area *
create_internal(void *place, size_t size, int tranche_id,
                dsm_handle control_handle, dsm_segment *control_segment,
                size_t init_segment_size, size_t max_segment_size) {
    dsa_area_control *control;
    dsa_area *area;
    dsa_segment_map *segment_map;
    size_t usable_pages, total_pages, metadata_bytes;
    int i;

    // Validate input parameters
    Assert(init_segment_size >= DSA_MIN_SEGMENT_SIZE);
    Assert(max_segment_size >= init_segment_size);
    Assert(max_segment_size <= DSA_MAX_SEGMENT_SIZE);

    if (size < dsa_minimum_size()) {
        elog(ERROR, "dsa_area space must be at least %zu, but %zu provided",
             dsa_minimum_size(), size);
    }

    // Calculate memory layout
    total_pages = size / FPM_PAGE_SIZE;
    metadata_bytes = MAXALIGN(sizeof(dsa_area_control)) +
                     MAXALIGN(sizeof(FreePageManager)) +
                     total_pages * sizeof(dsa_pointer);
    // Round up to page boundary
    if (metadata_bytes % FPM_PAGE_SIZE != 0) {
        metadata_bytes += FPM_PAGE_SIZE - (metadata_bytes % FPM_PAGE_SIZE);
    }
    usable_pages = (size - metadata_bytes) / FPM_PAGE_SIZE;

    // Initialize control structure in shared memory
    control = (dsa_area_control *) place;
    memset(place, 0, sizeof(*control));
    control->segment_header.magic = DSA_SEGMENT_HEADER_MAGIC ^ control_handle ^ 0;
    control->handle = control_handle;
    control->init_segment_size = init_segment_size;
    control->max_segment_size = max_segment_size;
    control->total_segment_size = size;
    control->refcnt = 1;
    control->lwlock_tranche_id = tranche_id;

    // Initialize segment bins (all empty initially)
    for (i = 0; i < DSA_NUM_SEGMENT_BINS; ++i) {
        control->segment_bins[i] = DSA_SEGMENT_INDEX_NONE;
    }

    // Create local area object for this backend
    area = palloc(sizeof(dsa_area));
    area->control = control;
    area->resowner = CurrentResourceOwner;
    memset(area->segment_maps, 0, sizeof(dsa_segment_map) * DSA_MAX_SEGMENTS);

    // Initialize locks
    LWLockInitialize(&control->lock, control->lwlock_tranche_id);
    for (i = 0; i < DSA_NUM_SIZE_CLASSES; ++i) {
        LWLockInitialize(DSA_SCLASS_LOCK(area, i), control->lwlock_tranche_id);
    }

    // Set up segment map for this process
    segment_map = &area->segment_maps[0];
    segment_map->segment = control_segment;
    segment_map->mapped_address = place;
    segment_map->header = (dsa_segment_header *) place;
    segment_map->fpm = (FreePageManager *)(segment_map->mapped_address +
                                          MAXALIGN(sizeof(dsa_area_control)));

    // Initialize free page manager
    FreePageManagerInitialize(segment_map->fpm, segment_map->mapped_address);
    if (usable_pages > 0) {
        FreePageManagerPut(segment_map->fpm, metadata_bytes / FPM_PAGE_SIZE, usable_pages);
    }

    // Place segment in appropriate bin
    control->segment_bins[contiguous_pages_to_segment_bin(usable_pages)] = 0;

    return area;
}
```

Key simplifications made:
- Condensed the memory layout calculation into clear steps
- Simplified the control structure initialization by focusing on key fields
- Abstracted some detailed header setup while preserving essential logic
- Emphasized the dual setup: shared control structure + local area object
- Focused on the core pattern: validate → calculate layout → initialize shared → create local → setup memory management