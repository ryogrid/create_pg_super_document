# dsm_set_control_handle

## Location
src/backend/storage/ipc/dsm.c: 459 - 469

## Overview
A callback function used under EXEC_BACKEND to set the DSM control handle when the main shared memory segment is re-attached.

## Definition
```c
void dsm_set_control_handle(dsm_handle h)
```

## Detailed Description
This function serves as a callback mechanism specifically for EXEC_BACKEND builds where backend processes are started as separate executables rather than forked from the postmaster. When these backend processes re-attach to the main shared memory segment, they need to retrieve and store the DSM control handle that was previously established by the postmaster.

The function performs a simple but critical task: it stores the provided control handle in the global dsm_control_handle variable. This handle is essential for subsequent DSM operations as it identifies the control segment that manages all dynamic shared memory segments in the system.

The function includes an assertion to ensure that it's only called once (when dsm_control_handle is still 0) and that a valid non-zero handle is being set, providing basic validation of the calling sequence.

## Parameters / Member Variables
- `h`: The DSM handle for the control segment to be stored

## Dependencies
- Functions called/Symbols referenced:
  - Assert
  - dsm_handle (type)
- Called from (representative examples):
  - PGSharedMemoryReAttach (both sysv_shmem.c and win32_shmem.c)

## Notes and Other Information
- Only relevant under EXEC_BACKEND builds; not used in standard Unix fork-based backends
- Function must be called exactly once during backend startup process
- The assertion ensures the function isn't called multiple times or with invalid handles
- Critical for proper DSM functionality in Windows and other EXEC_BACKEND environments
- The control handle is later used by dsm_backend_startup() to attach to the control segment
- Part of the shared memory re-attachment callback mechanism in PostgreSQL's memory management