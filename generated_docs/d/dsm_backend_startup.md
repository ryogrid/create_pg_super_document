# dsm_backend_startup

## Location
[src/backend/storage/ipc/dsm.c:423-458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L423-L458)

## Overview
Initializes dynamic shared memory functionality for a backend process by attaching to the control segment when running under EXEC_BACKEND mode.

## Definition
```c
static void dsm_backend_startup(void)
```

## Detailed Description
This function prepares a backend process for using dynamic shared memory by ensuring it has access to the DSM control segment. The behavior differs depending on the PostgreSQL build configuration:

Under EXEC_BACKEND builds (typically Windows), backend processes are started as separate executables and don't inherit the postmaster's memory mappings. In this case, the function must explicitly attach to the DSM control segment using the stored control handle and verify its integrity.

In non-EXEC_BACKEND builds (most Unix systems), backend processes are forked from the postmaster and inherit the memory mappings and global variables, so no additional setup is required.

The function performs sanity checks on the control segment after attachment to ensure it's valid. If corruption is detected, it detaches from the segment and reports a fatal error, as a corrupted control segment indicates a serious system problem.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [dsm_impl_op](dsm_impl_op.md) (with DSM_OP_ATTACH and DSM_OP_DETACH)
  - [dsm_control_segment_sane](dsm_control_segment_sane.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - Assert
  - IsUnderPostmaster
- Called from (representative examples):
  - [dsm_create](dsm_create.md)
  - [dsm_attach](dsm_attach.md)

## Notes and Other Information
- Only performs actual work under EXEC_BACKEND builds; otherwise just sets dsm_init_done flag
- Uses ERROR level for initial attachment operation to ensure proper error handling
- Uses WARNING level for detachment during error cleanup to avoid nested errors
- Reports FATAL error if control segment sanity check fails, indicating serious corruption
- Sets the global dsm_init_done flag to true to indicate DSM system is ready for use
- Assumes dsm_control_handle is already set by postmaster startup process