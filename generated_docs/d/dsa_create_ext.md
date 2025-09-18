# dsa_create_ext

## Location
src/backend/utils/mmgr/dsa.c: 421 - 470

## Overview
Creates a new Dynamic Shared Area (DSA) in a new Dynamic Shared Memory (DSM) segment with extended parameters for controlling segment sizes and LWLock tranche management.

## Definition


## Detailed Description
This function is the extended version of DSA creation that provides fine-grained control over memory allocation parameters. It creates a new DSA by first allocating a DSM segment to hold the shared control object and the initial usable space. The function implements explicit lifetime management by pinning all segments, ensuring that DSA can control when segments are freed rather than relying on backend-specific cleanup.

The function requires a caller-provided LWLock tranche ID because these are scarce resources (limited to 64k) that need careful management and cannot be recycled. After creating the DSM segment, it pins it to prevent premature cleanup, then delegates to create_internal() for the actual DSA setup. Finally, it registers a cleanup callback that will be invoked when the control segment detaches.

## Parameters / Member Variables
- : LWLock tranche identifier provided by caller (scarce resource, must be managed externally)
- : Size of the initial DSM segment containing control object and first usable space
- : Maximum size for additional segments that will be allocated as the DSA grows

## Dependencies
- Functions called/Symbols referenced:
  - dsm_create
  - dsm_pin_segment
  - create_internal
  - dsm_segment_address
  - dsm_segment_handle
  - on_dsm_detach
  - dsa_on_dsm_detach_release_in_place
- Called from (representative examples):
  - TidStoreCreateShared
  - dsa_create (wrapper function)

## Notes and Other Information
- All segments are pinned to allow DSA explicit control over segment lifetime
- LWLock tranche IDs are limited to 64k and cannot be recycled, so caller must manage them
- Registers cleanup callback for proper resource management when control segment detaches
- Part of PostgreSQL's shared memory management infrastructure
- Located in src/backend/utils/mmgr/dsa.c:421-470