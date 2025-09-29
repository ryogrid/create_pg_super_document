# init_span

## Location
[src/backend/utils/mmgr/dsa.c:1377-1431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1377-L1431)

## Overview
Initializes a new span structure and adds it to fullness class 1 of a specified DSA pool for managing small object allocations within the Dynamic Shared Area.

## Definition

```c
static void
init_span(dsa_area *area,
		  dsa_pointer span_pointer,
		  dsa_area_pool *pool, dsa_pointer start, size_t npages,
		  uint16 size_class)
```
## Detailed Description
This function initializes a newly created span structure that will manage allocations of objects within a specific size class. A span represents a contiguous block of memory pages that is subdivided into fixed-size objects. The function sets up the span's metadata, links it into the appropriate pool's span list at fullness class 1 (partially full), and calculates the number of allocatable objects based on the size class.

The function handles special cases for different span types: block-of-spans (which contain their own descriptors), large spans, and regular spans. It maintains the doubly-linked list structure that enables efficient span management and ensures proper initialization of allocation tracking fields.

## Parameters / Member Variables
- : Pointer to the DSA area containing the span
- : DSA pointer to the span structure to initialize
- : Pointer to the pool that will contain this span
- : DSA pointer to the beginning of the span's data area
- : Number of pages covered by this span
- : Size class identifier determining object size

## Dependencies
- Functions called/Symbols referenced:
  - [dsa_get_address](../d/dsa_get_address.md)
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md)
  - DSA_SCLASS_LOCK
  - DsaPointerIsValid
  - DsaAreaPoolToDsaPointer
  - dsa_size_classes array
  - DSA_SCLASS_BLOCK_OF_SPANS
  - DSA_SCLASS_SPAN_LARGE
  - DSA_SUPERBLOCK_SIZE
  - FPM_PAGE_SIZE
  - DSA_SPAN_NOTHING_FREE
  - InvalidDsaPointer
- Called from (representative examples):
  - [dsa_allocate_extended](../d/dsa_allocate_extended.md)
  - [ensure_active_superblock](../e/ensure_active_superblock.md)

## Notes and Other Information
- This is a static internal function used for DSA span management
- Requires the size class lock to be held by the calling thread
- Initializes spans to fullness class 1 (partially full) rather than class 0 (empty)
- Handles three distinct span types with different allocation calculations:
  - Block-of-spans: Reserves one object slot for the span descriptor itself
  - Large spans: Used for allocations larger than superblock subdivision
  - Regular spans: Standard fixed-size object allocation within superblocks
- Sets up doubly-linked list pointers for efficient span list management
- Critical for establishing the foundation of fine-grained memory allocation within DSA segments
- The nallocatable field calculation varies by size class to optimize memory utilization

## Simplified Source

```c
static void
init_span(dsa_area *area, dsa_pointer span_pointer, dsa_area_pool *pool,
          dsa_pointer start, size_t npages, uint16 size_class)
{
    dsa_area_span *span = dsa_get_address(area, span_pointer);
    size_t obsize = dsa_size_classes[size_class];

    // Must hold per-pool lock when manipulating span lists
    Assert(LWLockHeldByMe(DSA_SCLASS_LOCK(area, size_class)));

    // Link span into front of fullness class 1 list
    if (DsaPointerIsValid(pool->spans[1])) {
        dsa_area_span *head = (dsa_area_span *) dsa_get_address(area, pool->spans[1]);
        head->prevspan = span_pointer;
    }

    // Initialize span linkage
    span->pool = DsaAreaPoolToDsaPointer(area, pool);
    span->nextspan = pool->spans[1];
    span->prevspan = InvalidDsaPointer;
    pool->spans[1] = span_pointer;

    // Set basic span properties
    span->start = start;
    span->npages = npages;
    span->size_class = size_class;
    span->ninitialized = 0;

    // Calculate allocatable objects based on size class
    if (size_class == DSA_SCLASS_BLOCK_OF_SPANS) {
        // Block-of-spans: reserve one slot for descriptor
        span->ninitialized = 1;
        span->nallocatable = FPM_PAGE_SIZE / obsize - 1;
    } else if (size_class != DSA_SCLASS_SPAN_LARGE) {
        // Regular spans: use superblock size
        span->nallocatable = DSA_SUPERBLOCK_SIZE / obsize;
    }

    span->firstfree = DSA_SPAN_NOTHING_FREE;
    span->nmax = span->nallocatable;
    span->fclass = 1;  // Place in fullness class 1
}
```