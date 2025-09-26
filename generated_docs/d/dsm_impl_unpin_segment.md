# dsm_impl_unpin_segment

## Location
src/backend/storage/ipc/dsm_impl.c: 1014 - 1046

## Overview
Performs implementation-specific cleanup actions when a dynamic shared memory segment is no longer pinned and can be cleaned up after all backends detach.

## Definition

```c
void
dsm_impl_unpin_segment(dsm_handle handle, void **impl_private)
```
## Detailed Description
The  function reverses the actions performed by  to allow proper cleanup of dynamic shared memory segments. This function is primarily relevant on Windows platforms where segment handles need explicit management.

On Windows, the function closes the extra handle that was duplicated into the postmaster process during the pin operation. It uses  with the  flag to close the source handle in the postmaster's process space. This removes the reference that was keeping the segment alive, allowing Windows to clean up the segment once all backend processes have detached.

On non-Windows platforms, this function performs no operations since those systems don't require special handling for segment lifetime management.

## Parameters / Member Variables
- : Unique identifier for the shared memory segment to be unpinned
- : Pointer to implementation-specific private data containing the postmaster handle to be closed

## Dependencies  
- Functions called/Symbols referenced:
  - DuplicateHandle (Windows API)
  - PostmasterHandle, GetLastError (Windows API)
  - _dosmaperr, ereport  
  - errcode_for_dynamic_shared_memory
  - SEGMENT_NAME_PREFIX
- Called from (representative examples):
  - dsm_unpin_segment

## Notes and Other Information
- Only performs actual work on Windows platforms when  is defined
- Uses  flag to close the source handle without creating a new one
- Sets  to NULL after successful cleanup
- Essential counterpart to  for proper resource management
- Allows segments to be cleaned up automatically by Windows once all backend references are removed
- The implementation is conditional on  being 