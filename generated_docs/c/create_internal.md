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
  - dsa_minimum_size
  - FreePageManagerInitialize
  - FreePageManagerPut
  - LWLockInitialize
  - DSA_SCLASS_LOCK
  - contiguous_pages_to_segment_bin
  - palloc
  - elog/ERROR
- Called from (representative examples):
  - dsa_create_ext
  - dsa_create_in_place_ext

## Notes and Other Information
- This is a static internal function not exposed in the public DSA API
- Validates that provided size meets minimum DSA requirements via dsa_minimum_size()
- Initializes all segment bins to DSA_SEGMENT_INDEX_NONE initially
- Sets up both the main DSA lock and individual size class locks for concurrent access
- The function handles both shared memory and in-place memory scenarios through the same interface
- Metadata overhead includes space for dsa_area_control, FreePageManager, and page mapping structures
- Critical for establishing the foundation of PostgreSQL's dynamic shared memory allocation system