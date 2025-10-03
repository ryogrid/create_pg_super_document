# dsm_impl_unpin_segment

## Location
[src/backend/storage/ipc/dsm_impl.c:1014-1046](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm_impl.c#L1014-L1046)

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
- `handle`: Unique identifier for the shared memory segment to be unpinned
- `**impl_private`: Pointer to implementation-specific private data containing the postmaster handle to be closed
## Dependencies  
- Functions called/Symbols referenced:
  - DuplicateHandle (Windows API)
  - PostmasterHandle, GetLastError (Windows API)
  - [_dosmaperr](_dosmaperr.md), ereport  
  - [errcode_for_dynamic_shared_memory](../e/errcode_for_dynamic_shared_memory.md)
  - SEGMENT_NAME_PREFIX
- Called from (representative examples):
  - [dsm_unpin_segment](dsm_unpin_segment.md)

## Notes and Other Information
- Only performs actual work on Windows platforms when  is defined
- Uses DUPLICATE_CLOSE_SOURCE flag to close the source handle without creating a new one
- Sets impl_private to NULL after successful cleanup
- Essential counterpart to dsm_impl_pin_segment for proper resource management
- Allows segments to be cleaned up automatically by Windows once all backend references are removed
- The implementation is conditional on USE_DSM_WINDOWS being defined

## Simplified Source

```c
void
dsm_impl_unpin_segment(dsm_handle handle, void **impl_private)
{
    switch (dynamic_shared_memory_type) {
#ifdef USE_DSM_WINDOWS
        case DSM_IMPL_WINDOWS:
            if (IsUnderPostmaster) {
                // Close the postmaster handle that was keeping the segment alive
                if (*impl_private &&
                    !DuplicateHandle(PostmasterHandle, *impl_private,
                                   NULL, NULL, 0, FALSE,
                                   DUPLICATE_CLOSE_SOURCE)) {
                    char name[64];
                    snprintf(name, 64, "%s.%u", SEGMENT_NAME_PREFIX, handle);
                    _dosmaperr(GetLastError());
                    ereport(ERROR,
                           (errcode_for_dynamic_shared_memory(),
                            errmsg("could not duplicate handle for \"%s\": %m", name)));
                }

                // Clear the handle pointer
                *impl_private = NULL;
            }
            break;
#endif
        default:
            // No action needed on non-Windows platforms
            break;
    }
}
``` 