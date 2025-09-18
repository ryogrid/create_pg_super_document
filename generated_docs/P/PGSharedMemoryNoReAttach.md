# PGSharedMemoryNoReAttach

## Location
src/backend/port/sysv_shmem.c: 939 - 969

## Overview
Cleans up shared memory state when a postmaster child process chooses not to re-attach to the existing shared memory segment in EXEC_BACKEND configurations.

## Definition


## Detailed Description
This function is the counterpart to PGSharedMemoryReAttach(), called when a postmaster child process in an EXEC_BACKEND configuration decides not to re-attach to the shared memory segment created by the postmaster. It performs necessary cleanup to ensure the process state is consistent and that any subsequent calls to PGSharedMemoryDetach() will be safe no-ops.

The function performs the following operations:
1. On Cygwin systems, explicitly detaches from any existing shared memory attachment due to cygipc behavior with exec()
2. Resets the global UsedShmemSegAddr to NULL to indicate no attachment
3. Resets the global UsedShmemSegID to 0 for cleanliness

The function is designed to be safe and ensure that the process is in a clean state where it's clear that no shared memory is attached, preventing any confusion or errors in subsequent operations.

## Parameters / Member Variables
This function takes no parameters but modifies global variables:
- : Reset to NULL to indicate no attachment
- : Reset to 0 for cleanliness

## Dependencies
- Functions called/Symbols referenced:
  - PGSharedMemoryDetach (for Cygwin cleanup)
- Called from:
  - SubPostmasterMain

## Notes and Other Information
- Only used in EXEC_BACKEND configurations, similar to PGSharedMemoryReAttach()
- Requires Assert(UsedShmemSegAddr != NULL) and Assert(IsUnderPostmaster) to ensure proper calling context
- Designed to make subsequent PGSharedMemoryDetach() calls safe no-ops
- Special handling for Cygwin systems due to cygipc behavior
- The function ensures clean process state when shared memory attachment is not desired
- Part of the shared memory management strategy for processes that don't inherit memory segments through fork()