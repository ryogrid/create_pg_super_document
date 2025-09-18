# dsa_on_shmem_exit_release_in_place

## Location
src/backend/utils/mmgr/dsa.c: 590 - 604

## Overview
A callback function that automatically releases in-place DSA areas at backend process exit, providing cleanup for DSA areas in shared memory outside of DSM segments.

## Definition
```c
void dsa_on_shmem_exit_release_in_place(int code, Datum place)
```

## Detailed Description
This function serves as a shared memory exit callback that automatically releases DSA areas created with dsa_create_in_place or dsa_attach_in_place when a backend process exits. It is designed to be compatible with the on_shmem_exit and before_shmem_exit callback interfaces, accepting an exit code parameter (which is ignored) and a Datum containing the memory address of the DSA area.

The function is specifically intended for DSA areas created in shared memory that is not managed by DSM segments. While dsa_on_dsm_detach_release_in_place handles cleanup for DSA areas within DSM segments, this function provides cleanup for DSA areas in other types of shared memory arrangements.

## Parameters / Member Variables
- `code`: Exit code from the backend process (parameter ignored but required for callback interface compatibility)
- `place`: Datum containing the memory address where the DSA area was created

## Dependencies
- Functions called/Symbols referenced:
  - [dsa_release_in_place](dsa_release_in_place.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
- Called from (representative examples):
  - Referenced in DSA_HANDLE_INVALID (src/include/utils/dsa.h:151)

## Notes and Other Information
- Designed for use with on_shmem_exit or before_shmem_exit callback registration
- The code parameter is ignored but maintained for interface compatibility with shared memory exit callbacks
- Specifically intended for DSA areas in shared memory other than DSM segments
- The place parameter must contain the exact memory address where the DSA area was originally created
- Provides automatic cleanup of DSA areas when backend processes terminate unexpectedly or normally
- Complements dsa_on_dsm_detach_release_in_place for different shared memory scenarios
- Ensures proper resource cleanup even if the application doesn't explicitly release the DSA area before exit