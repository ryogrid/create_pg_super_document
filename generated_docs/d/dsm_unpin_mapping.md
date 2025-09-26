# dsm_unpin_mapping

## Location
src/backend/storage/ipc/dsm.c: 934 - 954

## Overview
Reverses the effect of dsm_pin_mapping by placing a dynamic shared memory mapping back under resource owner control, allowing it to be automatically cleaned up when the current resource owner is released.

## Definition
```c
void dsm_unpin_mapping(dsm_segment *seg)
```

## Detailed Description
The dsm_unpin_mapping function restores normal resource owner tracking for a previously pinned dynamic shared memory segment. It assigns the segment to the current resource owner by setting seg->resowner to CurrentResourceOwner and registering it with ResourceOwnerRememberDSM(). This ensures that the mapping will be automatically cleaned up when the current resource owner is released, typically at the end of the current query or transaction.

This function is useful when a segment that was pinned for session-level persistence is no longer needed for that duration and should revert to normal cleanup behavior. It's particularly important before performing operations that might invalidate the segment for future use by the current backend.

## Parameters / Member Variables
- `seg`: Pointer to the dsm_segment structure representing the dynamic shared memory segment to be unpinned. The segment must currently be pinned (resowner == NULL).

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerEnlarge
  - ResourceOwnerRememberDSM  
  - dsm_segment (structure type)
  - CurrentResourceOwner (global variable)
- Called from (representative examples):
  - Limited direct usage found in codebase (primarily used internally)

## Notes and Other Information
- The function includes an Assert(seg->resowner == NULL) to ensure the segment is currently pinned
- ResourceOwnerEnlarge() is called first to ensure the resource owner has capacity for the new DSM reference
- This operation is the inverse of dsm_pin_mapping() and restores normal resource management
- Should be used carefully to avoid premature cleanup of shared memory segments that are still in use
- Once unpinned, the segment's lifetime is tied to the current resource owner's lifecycle