# dsm_impl_pin_segment

## Location
[src/backend/storage/ipc/dsm_impl.c:963-1013](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm_impl.c#L963-L1013)

## Overview
Performs implementation-specific actions to preserve a dynamic shared memory segment when no backend process has it attached, primarily for Windows platforms.

## Definition

```c
void
dsm_impl_pin_segment(dsm_handle handle, void *impl_private,
					 void **impl_private_pm_handle)
```
## Detailed Description
The  function ensures that dynamic shared memory segments persist even when no backend processes are actively attached to them. This is primarily required on Windows systems, where the operating system automatically cleans up shared memory segments when no references remain.

On Windows, the function duplicates the segment handle into the postmaster process using the  API. This creates a reference that prevents Windows from cleaning up the segment prematurely. The duplicated handle is stored for later cleanup during unpinning operations.

On non-Windows platforms (Linux, Unix variants), this function performs no operations since those operating systems don't automatically clean up shared memory segments based on reference counts.

## Parameters / Member Variables
- : Unique identifier for the shared memory segment to be pinned
- : Implementation-specific private data (Windows HANDLE on Windows platforms)
- : Pointer to store the postmaster's copy of the handle (output parameter)

## Dependencies
- Functions called/Symbols referenced:
  - DuplicateHandle (Windows API)
  - GetCurrentProcess, GetLastError (Windows API)  
  - [_dosmaperr](_dosmaperr.md), ereport
  - [errcode_for_dynamic_shared_memory](../e/errcode_for_dynamic_shared_memory.md)
  - SEGMENT_NAME_PREFIX
- Called from (representative examples):
  - [dsm_pin_segment](dsm_pin_segment.md)

## Notes and Other Information
- Only performs actual work on Windows platforms when  is defined
- The duplicated handle in the postmaster is not usable in other processes but serves as a reference holder
- Uses DUPLICATE_SAME_ACCESS flag when duplicating handles
- Critical for preventing premature cleanup of pinned segments on Windows
- The implementation is conditional on USE_DSM_WINDOWS being defined

## Simplified Source

```c
void
dsm_impl_pin_segment(dsm_handle handle, void *impl_private,
                     void **impl_private_pm_handle)
{
    switch (dynamic_shared_memory_type) {
#ifdef USE_DSM_WINDOWS
        case DSM_IMPL_WINDOWS:
            if (IsUnderPostmaster) {
                HANDLE hmap;

                // Duplicate handle into postmaster to prevent cleanup
                if (!DuplicateHandle(GetCurrentProcess(), impl_private,
                                   PostmasterHandle, &hmap, 0, FALSE,
                                   DUPLICATE_SAME_ACCESS)) {
                    char name[64];
                    snprintf(name, 64, "%s.%u", SEGMENT_NAME_PREFIX, handle);
                    _dosmaperr(GetLastError());
                    ereport(ERROR,
                           (errcode_for_dynamic_shared_memory(),
                            errmsg("could not duplicate handle for \"%s\": %m", name)));
                }

                // Store postmaster handle for later cleanup
                *impl_private_pm_handle = hmap;
            }
            break;
#endif
        default:
            // No action needed on non-Windows platforms
            break;
    }
}
``` 