# dsm_impl_pin_segment

## Location
src/backend/storage/ipc/dsm_impl.c: 963 - 1013

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
  - _dosmaperr, ereport
  - errcode_for_dynamic_shared_memory
  - SEGMENT_NAME_PREFIX
- Called from (representative examples):
  - dsm_pin_segment

## Notes and Other Information
- Only performs actual work on Windows platforms when  is defined
- The duplicated handle in the postmaster is not usable in other processes but serves as a reference holder
- Uses  flag when duplicating handles
- Critical for preventing premature cleanup of pinned segments on Windows
- The implementation is conditional on  being 