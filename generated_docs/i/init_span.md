# init_span

## Location
src/backend/utils/mmgr/dsa.c: 1377 - 1431

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
  - dsa_get_address
  - LWLockHeldByMe
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
  - dsa_allocate_extended
  - ensure_active_superblock

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