# dsa_create_ext

## Location
[src/backend/utils/mmgr/dsa.c:421-470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L421-L470)

## Overview
Creates a new Dynamic Shared Area (DSA) in a new Dynamic Shared Memory (DSM) segment with extended parameters for controlling segment sizes and LWLock tranche management.

## Definition

```c
dsa_area *
dsa_create_ext(int tranche_id, size_t init_segment_size, size_t max_segment_size)
```
## Detailed Description
This function is the extended version of DSA creation that provides fine-grained control over memory allocation parameters. It creates a new DSA by first allocating a DSM segment to hold the shared control object and the initial usable space. The function implements explicit lifetime management by pinning all segments, ensuring that DSA can control when segments are freed rather than relying on backend-specific cleanup.

The function requires a caller-provided LWLock tranche ID because these are scarce resources (limited to 64k) that need careful management and cannot be recycled. After creating the DSM segment, it pins it to prevent premature cleanup, then delegates to create_internal() for the actual DSA setup. Finally, it registers a cleanup callback that will be invoked when the control segment detaches.

## Parameters / Member Variables
- `tranche_id`: LWLock tranche identifier provided by caller (scarce resource, must be managed externally)
- `init_segment_size`: Size of the initial DSM segment containing control object and first usable space
- `max_segment_size`: Maximum size for additional segments that will be allocated as the DSA grows
## Dependencies
- Functions called/Symbols referenced:
  - [dsm_create](dsm_create.md)
  - [dsm_pin_segment](dsm_pin_segment.md)
  - [create_internal](../c/create_internal.md)
  - [dsm_segment_address](dsm_segment_address.md)
  - [dsm_segment_handle](dsm_segment_handle.md)
  - [on_dsm_detach](../o/on_dsm_detach.md)
  - [dsa_on_dsm_detach_release_in_place](dsa_on_dsm_detach_release_in_place.md)
- Called from (representative examples):
  - [TidStoreCreateShared](../T/TidStoreCreateShared.md)
  - dsa_create (wrapper function)

## Notes and Other Information
- All segments are pinned to allow DSA explicit control over segment lifetime
- [LWLock](../L/LWLock.md) tranche IDs are limited to 64k and cannot be recycled, so caller must manage them
- Registers cleanup callback for proper resource management when control segment detaches
- Part of PostgreSQL's shared memory management infrastructure
- Located in src/backend/utils/mmgr/dsa.c:421-470

## Simplified Source

```c
// Simplified version of dsa_create_ext
dsa_area *
dsa_create_ext(int tranche_id, size_t init_segment_size, size_t max_segment_size) {
    dsm_segment *segment;
    dsa_area *area;

    // Create initial DSM segment for control object and first usable space
    segment = dsm_create(init_segment_size, 0);

    // Pin segment so DSA controls lifetime explicitly
    // (prevents premature cleanup when backends detach)
    dsm_pin_segment(segment);

    // Create DSA area with control object in this segment
    area = create_internal(dsm_segment_address(segment),
                          init_segment_size,
                          tranche_id,
                          dsm_segment_handle(segment), segment,
                          init_segment_size, max_segment_size);

    // Register cleanup callback for when control segment detaches
    on_dsm_detach(segment, &dsa_on_dsm_detach_release_in_place,
                  PointerGetDatum(dsm_segment_address(segment)));

    return area;
}
```

Key simplifications made:
- Added clear comments explaining each major step
- Condensed variable declarations for clarity
- Emphasized the key concept of segment pinning for lifetime control
- Simplified the create_internal call presentation
- Focused on the core pattern: create segment → pin → initialize DSA → register cleanup